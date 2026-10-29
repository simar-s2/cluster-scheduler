package clusterscheduler.controlplane.leadership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import clusterscheduler.controlplane.TestDatabase;
import clusterscheduler.controlplane.database.Database;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FencedTransactionsTest {
    private static final long SHORT_LEASE_MILLIS = 200;

    private Database database;
    private LeaseRepository leases;
    private FencedTransactions fenced;

    @BeforeEach
    void emptyDatabase() {
        TestDatabase.reset();
        database = TestDatabase.connect();
        leases = new LeaseRepository(database);
        fenced = new FencedTransactions(database);
    }

    @AfterEach
    void closeDatabase() {
        database.close();
    }

    @Test
    void theCurrentLeaderCanWrite() {
        long epoch = leases.tryAcquire("a", 60_000).getAsLong();

        fenced.run(epoch, connection -> insertMarkerJob(connection, "written"));

        assertEquals(1, countJobs("written"));
    }

    @Test
    void aReplacedLeaderCannotWriteAnything() throws InterruptedException {
        long oldEpoch = leases.tryAcquire("a", SHORT_LEASE_MILLIS).getAsLong();
        Thread.sleep(SHORT_LEASE_MILLIS + 100);
        leases.tryAcquire("b", 60_000);

        // "a" was paused and has just woken up, still believing it leads.
        assertThrows(StaleEpochException.class,
                () -> fenced.run(oldEpoch, connection -> insertMarkerJob(connection, "stale")));

        assertEquals(0, countJobs("stale"));
    }

    @Test
    void anExpiredLeaderCanStillWriteUntilSomeoneTakesOver() throws InterruptedException {
        // Safety comes from the epoch, not the clock: while nobody else has
        // started a term, the old term's writes cannot conflict with anyone's.
        long epoch = leases.tryAcquire("a", SHORT_LEASE_MILLIS).getAsLong();
        Thread.sleep(SHORT_LEASE_MILLIS + 100);

        fenced.run(epoch, connection -> insertMarkerJob(connection, "late-but-safe"));

        assertEquals(1, countJobs("late-but-safe"));
    }

    @Test
    void aTakeoverWaitsUntilTheOldLeadersTransactionCommits() throws Exception {
        long oldEpoch = leases.tryAcquire("a", SHORT_LEASE_MILLIS).getAsLong();
        Thread.sleep(SHORT_LEASE_MILLIS + 100);
        CountDownLatch insideFencedTransaction = new CountDownLatch(1);

        CompletableFuture<Void> oldLeaderWrite = CompletableFuture.runAsync(() ->
                fenced.run(oldEpoch, connection -> {
                    insertMarkerJob(connection, "committed-before-takeover");
                    insideFencedTransaction.countDown();
                    sleepQuietly(300);
                    return null;
                }));
        insideFencedTransaction.await(5, TimeUnit.SECONDS);

        long takeoverStarted = System.nanoTime();
        OptionalLong newEpoch = leases.tryAcquire("b", 60_000);
        long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - takeoverStarted);

        oldLeaderWrite.get(5, TimeUnit.SECONDS);
        assertEquals(OptionalLong.of(oldEpoch + 1), newEpoch);
        assertTrue(waitedMillis >= 150, "takeover did not wait for the lock, waited " + waitedMillis + " ms");
        assertEquals(1, countJobs("committed-before-takeover"));
    }

    private static Object insertMarkerJob(Connection connection, String jobId) throws SQLException {
        String sql = "INSERT INTO jobs (job_id, cpu_millis, memory_mb, duration_millis) VALUES (?, 1, 1, 0)";
        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            insert.setString(1, jobId);
            insert.executeUpdate();
        }
        return null;
    }

    private long countJobs(String jobId) {
        return database.inTransaction(connection -> {
            try (PreparedStatement query = connection.prepareStatement("SELECT count(*) FROM jobs WHERE job_id = ?")) {
                query.setString(1, jobId);
                try (ResultSet row = query.executeQuery()) {
                    row.next();
                    return row.getLong(1);
                }
            }
        });
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}

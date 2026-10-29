package clusterscheduler.controlplane.leadership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import clusterscheduler.controlplane.TestDatabase;
import clusterscheduler.controlplane.database.Database;
import java.util.OptionalLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeaseRepositoryTest {
    private static final long SHORT_LEASE_MILLIS = 300;

    private Database database;
    private LeaseRepository leases;

    @BeforeEach
    void emptyLease() {
        TestDatabase.reset();
        database = TestDatabase.connect();
        leases = new LeaseRepository(database);
    }

    @AfterEach
    void closeDatabase() {
        database.close();
    }

    @Test
    void theFirstReplicaToAskGetsEpochOne() {
        assertEquals(OptionalLong.of(1), leases.tryAcquire("a", SHORT_LEASE_MILLIS));
    }

    @Test
    void nobodyElseCanTakeAnUnexpiredLease() {
        leases.tryAcquire("a", 60_000);

        assertEquals(OptionalLong.empty(), leases.tryAcquire("b", 60_000));
        assertEquals(OptionalLong.empty(), leases.tryAcquire("a", 60_000));
    }

    @Test
    void onlyTheHolderCanRenewItsTerm() {
        long epoch = leases.tryAcquire("a", 60_000).getAsLong();

        assertTrue(leases.renew("a", epoch, 60_000));
        assertFalse(leases.renew("b", epoch, 60_000));
        assertFalse(leases.renew("a", epoch + 1, 60_000));
    }

    @Test
    void anExpiredLeaseGoesToANewTermAndTheOldHolderCannotRenew() throws InterruptedException {
        long firstEpoch = leases.tryAcquire("a", SHORT_LEASE_MILLIS).getAsLong();
        Thread.sleep(SHORT_LEASE_MILLIS + 100);

        long secondEpoch = leases.tryAcquire("b", 60_000).getAsLong();

        assertEquals(firstEpoch + 1, secondEpoch);
        assertFalse(leases.renew("a", firstEpoch, 60_000));
    }

    @Test
    void anExpiredLeaseIsNotRenewedEvenIfNobodyTookIt() throws InterruptedException {
        long epoch = leases.tryAcquire("a", SHORT_LEASE_MILLIS).getAsLong();
        Thread.sleep(SHORT_LEASE_MILLIS + 100);

        assertFalse(leases.renew("a", epoch, 60_000));
        assertEquals(OptionalLong.of(epoch + 1), leases.tryAcquire("a", 60_000));
    }
}

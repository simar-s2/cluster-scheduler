package clusterscheduler.controlplane.leadership;

import clusterscheduler.controlplane.database.Database;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.OptionalLong;

/**
 * Reads and writes the leader lease row. Every time comparison uses the
 * database clock (now()), so replicas with skewed clocks still agree on
 * whether the lease has expired.
 */
public final class LeaseRepository {
    private final Database database;

    public LeaseRepository(Database database) {
        this.database = database;
    }

    /**
     * Takes the lease if it has expired, starting a new term. Returns the new
     * epoch, or empty if someone else still holds an unexpired lease.
     *
     * The check and the write are one UPDATE, so two standbys racing for an
     * expired lease cannot both win: the row lock makes the second one wait,
     * and by then expires_at is in the future and its WHERE clause fails.
     */
    public OptionalLong tryAcquire(String replicaId, long leaseDurationMillis) {
        return database.inTransaction(connection -> {
            String sql = """
                    UPDATE leader_lease
                    SET holder = ?, epoch = epoch + 1, expires_at = now() + ? * interval '1 millisecond'
                    WHERE lease_id = 1 AND expires_at <= now()
                    RETURNING epoch
                    """;
            try (PreparedStatement update = connection.prepareStatement(sql)) {
                update.setString(1, replicaId);
                update.setLong(2, leaseDurationMillis);
                try (ResultSet row = update.executeQuery()) {
                    return row.next() ? OptionalLong.of(row.getLong("epoch")) : OptionalLong.empty();
                }
            }
        });
    }
}

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

    /**
     * Extends the lease this replica holds for this epoch. Returns false if
     * the lease has expired or belongs to a newer term, which means this
     * replica is no longer the leader.
     *
     * An expired lease is never renewed, even if nobody has taken it yet.
     * Getting leadership back after expiry goes through tryAcquire and starts
     * a new epoch, so "expired" always means "that term is over".
     */
    public boolean renew(String replicaId, long epoch, long leaseDurationMillis) {
        return database.inTransaction(connection -> {
            String sql = """
                    UPDATE leader_lease
                    SET expires_at = now() + ? * interval '1 millisecond'
                    WHERE lease_id = 1 AND holder = ? AND epoch = ? AND expires_at > now()
                    """;
            try (PreparedStatement update = connection.prepareStatement(sql)) {
                update.setLong(1, leaseDurationMillis);
                update.setString(2, replicaId);
                update.setLong(3, epoch);
                return update.executeUpdate() == 1;
            }
        });
    }
}

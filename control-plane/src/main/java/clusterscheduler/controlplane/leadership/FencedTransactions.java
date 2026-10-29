package clusterscheduler.controlplane.leadership;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.controlplane.database.TransactionWork;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Runs a leader's writes so that they commit only while its epoch is still
 * the current one. This is the fencing token check, done by PostgreSQL.
 *
 * Every transaction starts by reading the lease's epoch with FOR SHARE:
 *
 * - If the epoch has moved on, a newer leader exists. The transaction throws
 *   StaleEpochException and writes nothing.
 * - If it matches, the row now holds a shared lock until commit. Any number
 *   of fenced transactions can share it, but a takeover, which UPDATEs the
 *   lease row, must wait until they have all committed.
 *
 * So terms never overlap in the database. Every write from epoch N commits
 * before epoch N+1 begins, or not at all. A leader that was paused and wakes
 * up believing it still leads cannot change anything.
 */
public final class FencedTransactions {
    private final Database database;

    public FencedTransactions(Database database) {
        this.database = database;
    }

    public <T> T run(long epoch, TransactionWork<T> work) {
        return database.inTransaction(connection -> {
            long currentEpoch = lockCurrentEpoch(connection);
            if (currentEpoch != epoch) {
                throw new StaleEpochException(epoch, currentEpoch);
            }
            return work.run(connection);
        });
    }

    private static long lockCurrentEpoch(Connection connection) throws SQLException {
        String sql = "SELECT epoch FROM leader_lease WHERE lease_id = 1 FOR SHARE";
        try (PreparedStatement query = connection.prepareStatement(sql);
                ResultSet row = query.executeQuery()) {
            row.next();
            return row.getLong("epoch");
        }
    }
}

package clusterscheduler.controlplane.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

/** A pool of PostgreSQL connections plus a helper for running transactions. */
public final class Database implements AutoCloseable {
    private final HikariDataSource dataSource;

    public Database(String jdbcUrl, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(2_000);
        // If a replica freezes in the middle of a transaction (a long GC pause,
        // or "docker pause"), its row locks would stay held for as long as it
        // is frozen. A fenced transaction holds a lock on the lease row, so a
        // frozen leader would block every takeover. PostgreSQL ends such a
        // session after 2 seconds of silence and releases its locks.
        config.setConnectionInitSql("SET idle_in_transaction_session_timeout = '2s'");
        this.dataSource = new HikariDataSource(config);
    }

    /**
     * Runs the work in one transaction. Commits if it returns normally and rolls
     * back if it throws, so a failed operation never leaves half its writes behind.
     */
    public <T> T inTransaction(TransactionWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException failure) {
                rollbackQuietly(connection, failure);
                throw failure;
            }
        } catch (SQLException failure) {
            throw new DatabaseException("Transaction failed: " + failure.getMessage(), failure);
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }

    private static void rollbackQuietly(Connection connection, Exception originalFailure) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            originalFailure.addSuppressed(rollbackFailure);
        }
    }
}

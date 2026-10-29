package clusterscheduler.controlplane;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.controlplane.database.SchemaInitializer;
import java.sql.Statement;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * One real PostgreSQL container shared by every test in the run. Starting a
 * container takes seconds, so tests reset the tables instead of starting a
 * new one each time. Testcontainers removes the container when the JVM exits.
 */
final class TestDatabase {
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6");

    static {
        POSTGRES.start();
    }

    private TestDatabase() {}

    static Database connect() {
        return new Database(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    static ControlPlaneConfig configForReplica(String replicaId) {
        return new ControlPlaneConfig(
                replicaId, 0, POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    /** Creates the schema if needed and deletes every row, so each test starts empty. */
    static void reset() {
        try (Database database = connect()) {
            SchemaInitializer.apply(database);
            database.inTransaction(connection -> {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("TRUNCATE nodes");
                }
                return null;
            });
        }
    }
}

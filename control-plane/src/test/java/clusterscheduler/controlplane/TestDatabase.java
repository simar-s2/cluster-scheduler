package clusterscheduler.controlplane;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.controlplane.database.SchemaInitializer;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
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

    /** Settings for an in-process replica: a random free port and this test database. */
    static ControlPlaneConfig configForReplica(String replicaId, Map<String, String> overrides) {
        Map<String, String> variables = new HashMap<>(overrides);
        variables.put("REPLICA_ID", replicaId);
        variables.put("GRPC_PORT", "0");
        variables.put("DATABASE_URL", POSTGRES.getJdbcUrl());
        variables.put("DATABASE_USER", POSTGRES.getUsername());
        variables.put("DATABASE_PASSWORD", POSTGRES.getPassword());
        return ControlPlaneConfig.fromVariables(variables);
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

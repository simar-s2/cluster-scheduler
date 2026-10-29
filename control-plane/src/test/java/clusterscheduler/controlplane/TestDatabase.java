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
public final class TestDatabase {
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6");

    static {
        POSTGRES.start();
    }

    private TestDatabase() {}

    public static Database connect() {
        return new Database(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    /**
     * Settings for an in-process replica: a random free port on 127.0.0.1 and
     * this test database. Binding to 127.0.0.1 rather than every interface
     * matters on macOS, where Docker Desktop listens on 127.0.0.1 ports in
     * the same range the OS hands out for port 0.
     */
    public static ControlPlaneConfig configForReplica(String replicaId, Map<String, String> overrides) {
        Map<String, String> variables = new HashMap<>(overrides);
        variables.put("REPLICA_ID", replicaId);
        variables.put("GRPC_BIND_HOST", "127.0.0.1");
        variables.put("GRPC_PORT", "0");
        variables.put("METRICS_PORT", "0");
        variables.put("DATABASE_URL", POSTGRES.getJdbcUrl());
        variables.put("DATABASE_USER", POSTGRES.getUsername());
        variables.put("DATABASE_PASSWORD", POSTGRES.getPassword());
        return ControlPlaneConfig.fromVariables(variables);
    }

    /** Creates the schema if needed and deletes every row, so each test starts empty. */
    public static void reset() {
        try (Database database = connect()) {
            SchemaInitializer.apply(database);
            database.inTransaction(connection -> {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("TRUNCATE assignments, jobs, nodes");
                    statement.execute(
                            "UPDATE leader_lease SET holder = NULL, epoch = 0, expires_at = '-infinity'");
                }
                return null;
            });
        }
    }
}

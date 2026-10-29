package clusterscheduler.controlplane.database;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Statement;

/** Creates the tables from schema.sql if they do not exist yet. */
public final class SchemaInitializer {
    // Any fixed number works; it only has to be the same in every replica.
    private static final long SCHEMA_LOCK_ID = 7_301_001;

    private SchemaInitializer() {}

    public static void apply(Database database) {
        String schema = readSchemaFile();
        database.inTransaction(connection -> {
            try (Statement statement = connection.createStatement()) {
                // Three replicas start at the same time. Two concurrent
                // "CREATE TABLE IF NOT EXISTS" calls can still collide inside
                // PostgreSQL, so replicas take turns. The lock is released
                // when this transaction ends.
                statement.execute("SELECT pg_advisory_xact_lock(" + SCHEMA_LOCK_ID + ")");
                statement.execute(schema);
            }
            return null;
        });
    }

    private static String readSchemaFile() {
        try (InputStream input = SchemaInitializer.class.getResourceAsStream("/schema.sql")) {
            if (input == null) {
                throw new IllegalStateException("schema.sql is missing from the classpath");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}

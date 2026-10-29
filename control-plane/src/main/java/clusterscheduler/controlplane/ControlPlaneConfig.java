package clusterscheduler.controlplane;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Settings for one control plane replica. */
public record ControlPlaneConfig(
        String replicaId,
        int grpcPort,
        String databaseUrl,
        String databaseUser,
        String databasePassword) {

    /** Reads the settings from environment variables, with defaults for local runs. */
    public static ControlPlaneConfig fromEnvironment() {
        return new ControlPlaneConfig(
                text("REPLICA_ID", hostname()),
                number("GRPC_PORT", 9090),
                text("DATABASE_URL", "jdbc:postgresql://localhost:5432/scheduler"),
                text("DATABASE_USER", "scheduler"),
                text("DATABASE_PASSWORD", "scheduler"));
    }

    private static String text(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static int number(String name, int defaultValue) {
        return Integer.parseInt(text(name, Integer.toString(defaultValue)));
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException failure) {
            return "control-plane";
        }
    }
}

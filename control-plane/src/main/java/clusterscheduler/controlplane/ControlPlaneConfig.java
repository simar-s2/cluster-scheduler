package clusterscheduler.controlplane;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/** Settings for one control plane replica. */
public record ControlPlaneConfig(
        String replicaId,
        String grpcBindHost,
        int grpcPort,
        int metricsPort,
        String databaseUrl,
        String databaseUser,
        String databasePassword,
        long schedulingIntervalMillis,
        int schedulingBatchSize,
        long reconcileIntervalMillis,
        long heartbeatTimeoutMillis,
        long resendAfterMillis,
        long leaseDurationMillis,
        long leaseRenewIntervalMillis) {

    /**
     * Reads the settings from a map of variables, such as System.getenv().
     * Anything missing gets a default suited to local runs, so tests only set
     * what they care about.
     */
    public static ControlPlaneConfig fromVariables(Map<String, String> variables) {
        Settings settings = new Settings(variables);
        return new ControlPlaneConfig(
                settings.text("REPLICA_ID", hostname()),
                settings.text("GRPC_BIND_HOST", "0.0.0.0"),
                (int) settings.number("GRPC_PORT", 9090),
                (int) settings.number("METRICS_PORT", 8080),
                settings.text("DATABASE_URL", "jdbc:postgresql://localhost:5432/scheduler"),
                settings.text("DATABASE_USER", "scheduler"),
                settings.text("DATABASE_PASSWORD", "scheduler"),
                settings.number("SCHEDULING_INTERVAL_MILLIS", 100),
                (int) settings.number("SCHEDULING_BATCH_SIZE", 1_000),
                settings.number("RECONCILE_INTERVAL_MILLIS", 1_000),
                // Five missed one-second heartbeats before a node counts as dead.
                settings.number("HEARTBEAT_TIMEOUT_MILLIS", 5_000),
                settings.number("RESEND_AFTER_MILLIS", 3_000),
                // A standby takes over within about this long after the leader dies.
                settings.number("LEASE_DURATION_MILLIS", 2_000),
                settings.number("LEASE_RENEW_INTERVAL_MILLIS", 500));
    }

    private record Settings(Map<String, String> variables) {
        String text(String name, String defaultValue) {
            String value = variables.get(name);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        long number(String name, long defaultValue) {
            return Long.parseLong(text(name, Long.toString(defaultValue)));
        }
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException failure) {
            return "control-plane";
        }
    }
}

package clusterscheduler.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Settings for one worker node. */
public record WorkerConfig(
        List<String> controlPlaneAddresses,
        String advertisedHost,
        String grpcBindHost,
        int grpcPort,
        long cpuCapacityMillis,
        long memoryCapacityMb,
        long heartbeatIntervalMillis) {

    /**
     * Reads the settings from a map of variables, such as System.getenv().
     * Anything missing gets a default suited to local runs.
     */
    public static WorkerConfig fromVariables(Map<String, String> variables) {
        Settings settings = new Settings(variables);
        return new WorkerConfig(
                Arrays.stream(settings.text("CONTROL_PLANE_ADDRESSES", "localhost:9090").split(","))
                        .map(String::trim)
                        .toList(),
                settings.text("ADVERTISED_HOST", localIpAddress()),
                settings.text("GRPC_BIND_HOST", "0.0.0.0"),
                (int) settings.number("GRPC_PORT", 9100),
                settings.number("CPU_CAPACITY_MILLIS", 4_000),
                settings.number("MEMORY_CAPACITY_MB", 8_192),
                settings.number("HEARTBEAT_INTERVAL_MILLIS", 1_000));
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

    /** Inside a container this is the container's IP, which the leader can dial. */
    private static String localIpAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException failure) {
            return "localhost";
        }
    }
}

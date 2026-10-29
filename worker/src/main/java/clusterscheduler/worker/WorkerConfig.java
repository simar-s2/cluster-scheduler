package clusterscheduler.worker;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/** Settings for one worker node. */
public record WorkerConfig(
        List<String> controlPlaneAddresses,
        String advertisedHost,
        int grpcPort,
        long cpuCapacityMillis,
        long memoryCapacityMb,
        long heartbeatIntervalMillis) {

    /** Reads the settings from environment variables, with defaults for local runs. */
    public static WorkerConfig fromEnvironment() {
        return new WorkerConfig(
                Arrays.stream(text("CONTROL_PLANE_ADDRESSES", "localhost:9090").split(","))
                        .map(String::trim)
                        .toList(),
                text("ADVERTISED_HOST", localIpAddress()),
                (int) number("GRPC_PORT", 9100),
                number("CPU_CAPACITY_MILLIS", 4_000),
                number("MEMORY_CAPACITY_MB", 8_192),
                number("HEARTBEAT_INTERVAL_MILLIS", 1_000));
    }

    private static String text(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static long number(String name, long defaultValue) {
        return Long.parseLong(text(name, Long.toString(defaultValue)));
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

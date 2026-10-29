package clusterscheduler.controlplane.state;

/**
 * A live node and how much of it is free. Free means capacity minus the
 * requests of the node's active assignments, as recorded in PostgreSQL.
 */
public record NodeSpace(
        String nodeId,
        String address,
        long cpuCapacityMillis,
        long memoryCapacityMb,
        long freeCpuMillis,
        long freeMemoryMb) {}

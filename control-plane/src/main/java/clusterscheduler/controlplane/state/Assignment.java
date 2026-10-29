package clusterscheduler.controlplane.state;

/** One placement of a job on a node, with everything needed to send it to the worker. */
public record Assignment(
        String assignmentId,
        String jobId,
        String nodeId,
        String nodeAddress,
        long cpuMillis,
        long memoryMb,
        long durationMillis) {}

package clusterscheduler.controlplane.state;

import java.util.List;

/** What a worker reports in one heartbeat. */
public record NodeHeartbeat(
        String nodeId,
        long freeCpuMillis,
        long freeMemoryMb,
        List<String> runningAssignmentIds,
        List<String> finishedAssignmentIds) {}

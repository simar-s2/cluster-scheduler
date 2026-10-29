package clusterscheduler.controlplane.state;

/** A job waiting in the queue for a node with enough free CPU and memory. */
public record PendingJob(String jobId, long cpuMillis, long memoryMb, long durationMillis) {}

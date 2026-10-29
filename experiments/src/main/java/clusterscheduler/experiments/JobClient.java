package clusterscheduler.experiments;

import clusterscheduler.proto.ControlPlaneGrpc;
import clusterscheduler.proto.Resources;
import clusterscheduler.proto.SubmitJobRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Submits jobs to the three replicas published on localhost. Like a worker,
 * it sticks to one replica and moves on when a call fails. A retry is always
 * safe because the job ID is chosen here, so resubmitting creates no duplicate.
 */
final class JobClient implements AutoCloseable {
    private static final List<String> REPLICAS = List.of("127.0.0.1:9091", "127.0.0.1:9092", "127.0.0.1:9093");
    private static final int MAX_ATTEMPTS = 30;

    private final List<ManagedChannel> channels = REPLICAS.stream()
            .map(address -> ManagedChannelBuilder.forTarget(address).usePlaintext().build())
            .toList();
    private int currentReplicaIndex;

    JobClient(int startingReplicaIndex) {
        this.currentReplicaIndex = startingReplicaIndex % REPLICAS.size();
    }

    /** Returns true once a replica has accepted the job, false if none did after every retry. */
    synchronized boolean submit(String jobId, long cpuMillis, long memoryMb, long durationMillis) {
        SubmitJobRequest request = SubmitJobRequest.newBuilder()
                .setJobId(jobId)
                .setRequested(Resources.newBuilder().setCpuMillis(cpuMillis).setMemoryMb(memoryMb))
                .setDurationMillis(durationMillis)
                .build();
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            try {
                ControlPlaneGrpc.newBlockingStub(channels.get(currentReplicaIndex))
                        .withDeadlineAfter(1, TimeUnit.SECONDS)
                        .submitJob(request);
                return true;
            } catch (StatusRuntimeException failure) {
                currentReplicaIndex = (currentReplicaIndex + 1) % REPLICAS.size();
            }
        }
        return false;
    }

    @Override
    public void close() {
        channels.forEach(ManagedChannel::shutdownNow);
    }
}

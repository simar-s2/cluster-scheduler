package clusterscheduler.worker;

import clusterscheduler.proto.ControlPlaneGrpc;
import clusterscheduler.proto.ControlPlaneGrpc.ControlPlaneBlockingStub;
import clusterscheduler.proto.HeartbeatRequest;
import clusterscheduler.proto.HeartbeatResponse;
import clusterscheduler.proto.RegisterNodeRequest;
import clusterscheduler.proto.RegisterNodeResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Calls the control plane. Any replica can answer these calls, so the client
 * sticks to one replica and moves on to the next one when a call fails.
 */
final class ControlPlaneClient implements AutoCloseable {
    private static final long CALL_TIMEOUT_MILLIS = 1_000;

    private final List<ManagedChannel> channels;
    private int currentReplicaIndex;

    ControlPlaneClient(List<String> replicaAddresses) {
        this.channels = replicaAddresses.stream()
                .map(address -> ManagedChannelBuilder.forTarget(address).usePlaintext().build())
                .toList();
        // Start at a random replica so the workers spread out across replicas.
        this.currentReplicaIndex = ThreadLocalRandom.current().nextInt(channels.size());
    }

    RegisterNodeResponse registerNode(RegisterNodeRequest request) {
        return callAnyReplica(stub -> stub.registerNode(request));
    }

    HeartbeatResponse heartbeat(HeartbeatRequest request) {
        return callAnyReplica(stub -> stub.heartbeat(request));
    }

    /** Tries each replica at most once, starting with the one that worked last. */
    private synchronized <T> T callAnyReplica(Function<ControlPlaneBlockingStub, T> call) {
        StatusRuntimeException lastFailure = null;
        for (int attempt = 0; attempt < channels.size(); attempt++) {
            ControlPlaneBlockingStub stub = ControlPlaneGrpc.newBlockingStub(channels.get(currentReplicaIndex))
                    .withDeadlineAfter(CALL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            try {
                return call.apply(stub);
            } catch (StatusRuntimeException failure) {
                if (!isWorthRetryingElsewhere(failure)) {
                    throw failure;
                }
                lastFailure = failure;
                currentReplicaIndex = (currentReplicaIndex + 1) % channels.size();
            }
        }
        throw lastFailure;
    }

    /** The replica is down, frozen or cannot reach PostgreSQL; another one may do better. */
    private static boolean isWorthRetryingElsewhere(StatusRuntimeException failure) {
        Status.Code code = failure.getStatus().getCode();
        return code == Status.Code.UNAVAILABLE || code == Status.Code.DEADLINE_EXCEEDED;
    }

    @Override
    public void close() {
        channels.forEach(ManagedChannel::shutdownNow);
    }
}

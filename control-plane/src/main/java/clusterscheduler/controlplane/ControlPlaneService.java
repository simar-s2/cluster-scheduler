package clusterscheduler.controlplane;

import clusterscheduler.controlplane.database.DatabaseException;
import clusterscheduler.controlplane.state.NodeRepository;
import clusterscheduler.proto.ControlPlaneGrpc;
import clusterscheduler.proto.HeartbeatRequest;
import clusterscheduler.proto.HeartbeatResponse;
import clusterscheduler.proto.RegisterNodeRequest;
import clusterscheduler.proto.RegisterNodeResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gRPC API every replica serves. Each call only records a fact in
 * PostgreSQL, so it works the same on the leader and on standbys.
 */
final class ControlPlaneService extends ControlPlaneGrpc.ControlPlaneImplBase {
    private static final Logger log = LoggerFactory.getLogger(ControlPlaneService.class);

    private final NodeRepository nodeRepository;

    ControlPlaneService(NodeRepository nodeRepository) {
        this.nodeRepository = nodeRepository;
    }

    @Override
    public void registerNode(RegisterNodeRequest request, StreamObserver<RegisterNodeResponse> responseObserver) {
        respond(responseObserver, () -> {
            requireThat(!request.getAddress().isBlank(), "address is required");
            requireThat(request.getCapacity().getCpuMillis() > 0, "capacity.cpu_millis must be positive");
            requireThat(request.getCapacity().getMemoryMb() > 0, "capacity.memory_mb must be positive");
            String nodeId = nodeRepository.register(
                    request.getAddress(),
                    request.getCapacity().getCpuMillis(),
                    request.getCapacity().getMemoryMb());
            log.info("Registered node {} at {}", nodeId, request.getAddress());
            return RegisterNodeResponse.newBuilder().setNodeId(nodeId).build();
        });
    }

    @Override
    public void heartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        respond(responseObserver, () -> {
            boolean nodeIsKnown = nodeRepository.recordHeartbeat(
                    request.getNodeId(),
                    request.getFree().getCpuMillis(),
                    request.getFree().getMemoryMb());
            return HeartbeatResponse.newBuilder().setMustReregister(!nodeIsKnown).build();
        });
    }

    /** Sends the result, or turns a failure into the matching gRPC status. */
    private static <T> void respond(StreamObserver<T> responseObserver, Supplier<T> handler) {
        try {
            responseObserver.onNext(handler.get());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException invalidRequest) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(invalidRequest.getMessage())
                    .asRuntimeException());
        } catch (DatabaseException databaseDown) {
            // UNAVAILABLE tells the client that retrying, here or on another
            // replica, is the right reaction.
            responseObserver.onError(Status.UNAVAILABLE
                    .withDescription(databaseDown.getMessage())
                    .asRuntimeException());
        }
    }

    private static void requireThat(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}

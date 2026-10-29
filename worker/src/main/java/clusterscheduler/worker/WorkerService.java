package clusterscheduler.worker;

import clusterscheduler.proto.AssignTaskRequest;
import clusterscheduler.proto.AssignTaskResponse;
import clusterscheduler.proto.AssignTaskResponse.Result;
import clusterscheduler.proto.WorkerGrpc;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The gRPC API the leader calls to hand this worker a task.
 *
 * Each request goes through three checks, in this order:
 * 1. Fencing: the sender's epoch must not be older than one already seen.
 * 2. Identity: the assignment must be for this worker's current node ID.
 * 3. Dedupe: an assignment ID that was seen before is not started again.
 *
 * The method is synchronized so the checks and the start happen as one step.
 * Otherwise an old leader's request could pass the epoch check just before a
 * newer leader's request raises the epoch, and still start afterwards.
 */
final class WorkerService extends WorkerGrpc.WorkerImplBase {
    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    private final TaskRunner taskRunner;
    private final NodeIdentity identity;
    private final EpochFence epochFence;

    WorkerService(TaskRunner taskRunner, NodeIdentity identity, EpochFence epochFence) {
        this.taskRunner = taskRunner;
        this.identity = identity;
        this.epochFence = epochFence;
    }

    @Override
    public synchronized void assignTask(AssignTaskRequest request, StreamObserver<AssignTaskResponse> responseObserver) {
        responseObserver.onNext(AssignTaskResponse.newBuilder().setResult(decide(request)).build());
        responseObserver.onCompleted();
    }

    private Result decide(AssignTaskRequest request) {
        if (!epochFence.admit(request.getLeaderEpoch())) {
            log.warn("FENCED: rejected assignment {} from stale epoch {}",
                    request.getAssignmentId(), request.getLeaderEpoch());
            return Result.RESULT_STALE_EPOCH;
        }
        if (!request.getNodeId().equals(identity.current())) {
            log.warn("Rejected assignment {}: it is for node {}, not this node",
                    request.getAssignmentId(), request.getNodeId());
            return Result.RESULT_WRONG_NODE;
        }
        boolean started = taskRunner.startIfNew(
                request.getAssignmentId(),
                request.getRequested().getCpuMillis(),
                request.getRequested().getMemoryMb(),
                request.getDurationMillis());
        return started ? Result.RESULT_STARTED : Result.RESULT_DUPLICATE;
    }
}

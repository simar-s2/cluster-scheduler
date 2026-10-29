package clusterscheduler.worker;

import clusterscheduler.proto.AssignTaskRequest;
import clusterscheduler.proto.AssignTaskResponse;
import clusterscheduler.proto.AssignTaskResponse.Result;
import clusterscheduler.proto.WorkerGrpc;
import io.grpc.stub.StreamObserver;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The gRPC API the leader calls to hand this worker a task. */
final class WorkerService extends WorkerGrpc.WorkerImplBase {
    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    private final TaskRunner taskRunner;
    private final Supplier<String> currentNodeId;

    WorkerService(TaskRunner taskRunner, Supplier<String> currentNodeId) {
        this.taskRunner = taskRunner;
        this.currentNodeId = currentNodeId;
    }

    @Override
    public synchronized void assignTask(AssignTaskRequest request, StreamObserver<AssignTaskResponse> responseObserver) {
        responseObserver.onNext(AssignTaskResponse.newBuilder().setResult(decide(request)).build());
        responseObserver.onCompleted();
    }

    private Result decide(AssignTaskRequest request) {
        if (!request.getNodeId().equals(currentNodeId.get())) {
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

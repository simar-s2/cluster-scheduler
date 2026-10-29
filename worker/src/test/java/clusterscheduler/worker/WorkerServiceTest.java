package clusterscheduler.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import clusterscheduler.proto.AssignTaskRequest;
import clusterscheduler.proto.AssignTaskResponse;
import clusterscheduler.proto.AssignTaskResponse.Result;
import clusterscheduler.proto.Resources;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkerServiceTest {
    private final TaskRunner taskRunner = new TaskRunner(4_000, 8_192, ExecutionLog.disabled());
    private final NodeIdentity identity = new NodeIdentity();
    private final WorkerService service = new WorkerService(taskRunner, identity, new EpochFence(), ExecutionLog.disabled());

    @BeforeEach
    void register() {
        identity.set("node-1");
    }

    @AfterEach
    void stopTimer() {
        taskRunner.close();
    }

    @Test
    void startsANewAssignment() {
        assertEquals(Result.RESULT_STARTED, assign("a1", "node-1", 1));
        assertEquals(List.of("a1"), taskRunner.runningAssignmentIds());
    }

    @Test
    void aRepeatedAssignmentIsADuplicateAndStartsNothing() {
        assign("a1", "node-1", 1);

        assertEquals(Result.RESULT_DUPLICATE, assign("a1", "node-1", 1));
        assertEquals(1, taskRunner.runningAssignmentIds().size());
    }

    @Test
    void anAssignmentFromAnOlderLeaderIsRejected() {
        assign("from-new-leader", "node-1", 5);

        assertEquals(Result.RESULT_STALE_EPOCH, assign("from-old-leader", "node-1", 4));
        assertEquals(List.of("from-new-leader"), taskRunner.runningAssignmentIds());
    }

    @Test
    void anAssignmentForAnotherNodeIsRejected() {
        assertEquals(Result.RESULT_WRONG_NODE, assign("a1", "node-that-used-this-address-before", 1));
        assertEquals(List.of(), taskRunner.runningAssignmentIds());
    }

    @Test
    void nothingIsAcceptedWhileTheWorkerIsBetweenRegistrations() {
        identity.clear();

        assertEquals(Result.RESULT_WRONG_NODE, assign("a1", "node-1", 1));
    }

    private Result assign(String assignmentId, String nodeId, long epoch) {
        AssignTaskRequest request = AssignTaskRequest.newBuilder()
                .setAssignmentId(assignmentId)
                .setJobId("job-" + assignmentId)
                .setNodeId(nodeId)
                .setLeaderEpoch(epoch)
                .setRequested(Resources.newBuilder().setCpuMillis(500).setMemoryMb(500))
                .setDurationMillis(60_000)
                .build();
        RecordingObserver observer = new RecordingObserver();
        service.assignTask(request, observer);
        return observer.responses.get(0).getResult();
    }

    private static final class RecordingObserver implements StreamObserver<AssignTaskResponse> {
        private final List<AssignTaskResponse> responses = new ArrayList<>();

        @Override
        public void onNext(AssignTaskResponse response) {
            responses.add(response);
        }

        @Override
        public void onError(Throwable failure) {
            throw new AssertionError(failure);
        }

        @Override
        public void onCompleted() {}
    }
}

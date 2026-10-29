package clusterscheduler.controlplane.delivery;

import clusterscheduler.controlplane.leadership.Leadership;
import clusterscheduler.controlplane.state.Assignment;
import clusterscheduler.proto.AssignTaskRequest;
import clusterscheduler.proto.AssignTaskResponse;
import clusterscheduler.proto.Resources;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends saved assignments to their workers. Sending happens on a pool of
 * threads, so a slow or dead worker never holds up the scheduling loop.
 *
 * A failed send is only logged. The assignment is already in PostgreSQL,
 * and the reconcile loop resends any assignment the worker has not confirmed.
 */
public final class AssignmentSender implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AssignmentSender.class);
    private static final long SEND_TIMEOUT_MILLIS = 2_000;

    private final Leadership leadership;
    private final WorkerChannels workerChannels = new WorkerChannels();
    private final ExecutorService senderThreads =
            Executors.newFixedThreadPool(16, runnable -> new Thread(runnable, "assignment-sender"));

    public AssignmentSender(Leadership leadership) {
        this.leadership = leadership;
    }

    /** Sends each assignment, stamped with the epoch of the leader sending it. */
    public void sendAll(List<Assignment> assignments, long leaderEpoch) {
        for (Assignment assignment : assignments) {
            senderThreads.execute(() -> send(assignment, leaderEpoch));
        }
    }

    /** Forgets the connection to a node that has been declared dead. */
    public void forgetNode(String nodeId) {
        workerChannels.close(nodeId);
    }

    private void send(Assignment assignment, long leaderEpoch) {
        AssignTaskRequest request = AssignTaskRequest.newBuilder()
                .setAssignmentId(assignment.assignmentId())
                .setJobId(assignment.jobId())
                .setNodeId(assignment.nodeId())
                .setRequested(Resources.newBuilder()
                        .setCpuMillis(assignment.cpuMillis())
                        .setMemoryMb(assignment.memoryMb()))
                .setDurationMillis(assignment.durationMillis())
                .setLeaderEpoch(leaderEpoch)
                .build();
        try {
            AssignTaskResponse response = workerChannels.stubFor(assignment.nodeId(), assignment.nodeAddress())
                    .withDeadlineAfter(SEND_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)
                    .assignTask(request);
            switch (response.getResult()) {
                case RESULT_STALE_EPOCH -> {
                    // The worker has heard from a newer leader, so this replica
                    // has been replaced even if its lease has not caught up yet.
                    log.warn("FENCED: worker {} rejected epoch {}; stepping down",
                            assignment.nodeId(), leaderEpoch);
                    leadership.stepDown(leaderEpoch);
                }
                case RESULT_WRONG_NODE -> log.warn("Address {} no longer belongs to node {}",
                        assignment.nodeAddress(), assignment.nodeId());
                default -> {
                    // Started, or already running there. Either way it is delivered.
                }
            }
        } catch (StatusRuntimeException failure) {
            log.debug("Could not send assignment {} to node {}: {}",
                    assignment.assignmentId(), assignment.nodeId(), failure.getStatus());
        }
    }

    @Override
    public void close() {
        senderThreads.shutdownNow();
        workerChannels.close();
    }
}

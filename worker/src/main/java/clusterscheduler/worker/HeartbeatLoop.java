package clusterscheduler.worker;

import clusterscheduler.proto.HeartbeatRequest;
import clusterscheduler.proto.HeartbeatResponse;
import clusterscheduler.proto.RegisterNodeRequest;
import clusterscheduler.proto.Resources;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registers the worker, then sends a heartbeat on a fixed interval. */
final class HeartbeatLoop implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatLoop.class);

    private final ControlPlaneClient controlPlane;
    private final TaskRunner taskRunner;
    private final NodeIdentity identity;
    private final String address;
    private final Resources capacity;
    private final long intervalMillis;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "heartbeat"));

    HeartbeatLoop(
            ControlPlaneClient controlPlane,
            TaskRunner taskRunner,
            NodeIdentity identity,
            String address,
            Resources capacity,
            long intervalMillis) {
        this.controlPlane = controlPlane;
        this.taskRunner = taskRunner;
        this.identity = identity;
        this.address = address;
        this.capacity = capacity;
        this.intervalMillis = intervalMillis;
    }

    void start() {
        timer.scheduleWithFixedDelay(this::beatAndLogFailures, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void beatAndLogFailures() {
        try {
            beat();
        } catch (RuntimeException failure) {
            // Swallowed on purpose: an exception would cancel the scheduled
            // task. The next tick tries again.
            log.warn("Heartbeat failed: {}", failure.getMessage());
        }
    }

    private void beat() {
        if (identity.current() == null) {
            register();
        }
        String nodeId = identity.current();
        TaskRunner.Snapshot snapshot = taskRunner.snapshot();
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setNodeId(nodeId)
                .setFree(Resources.newBuilder()
                        .setCpuMillis(snapshot.freeCpuMillis())
                        .setMemoryMb(snapshot.freeMemoryMb()))
                .addAllRunningAssignmentIds(snapshot.runningAssignmentIds())
                .addAllFinishedAssignmentIds(snapshot.finishedAssignmentIds())
                .build();
        HeartbeatResponse response = controlPlane.heartbeat(request);
        // Only now is it safe to forget these: the control plane has stored
        // them. If the call had failed, the next heartbeat would send them again.
        taskRunner.markFinishedReported(snapshot.finishedAssignmentIds());
        if (response.getMustReregister()) {
            log.warn("Control plane no longer knows node {}; registering again", nodeId);
            identity.clear();
        }
    }

    private void register() {
        RegisterNodeRequest request = RegisterNodeRequest.newBuilder()
                .setAddress(address)
                .setCapacity(capacity)
                .build();
        String nodeId = controlPlane.registerNode(request).getNodeId();
        identity.set(nodeId);
        log.info("Registered as node {} at {}", nodeId, address);
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}

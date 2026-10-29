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
    private final String address;
    private final Resources capacity;
    private final long intervalMillis;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "heartbeat"));

    // Null until the first successful registration. Only the timer thread writes it.
    private volatile String nodeId;

    HeartbeatLoop(ControlPlaneClient controlPlane, String address, Resources capacity, long intervalMillis) {
        this.controlPlane = controlPlane;
        this.address = address;
        this.capacity = capacity;
        this.intervalMillis = intervalMillis;
    }

    void start() {
        timer.scheduleWithFixedDelay(this::beatAndLogFailures, 0, intervalMillis, TimeUnit.MILLISECONDS);
    }

    /** The current node ID, or null while the worker is not registered. */
    String nodeId() {
        return nodeId;
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
        if (nodeId == null) {
            register();
        }
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setNodeId(nodeId)
                .setFree(capacity)
                .build();
        HeartbeatResponse response = controlPlane.heartbeat(request);
        if (response.getMustReregister()) {
            log.warn("Control plane no longer knows node {}; registering again", nodeId);
            nodeId = null;
        }
    }

    private void register() {
        RegisterNodeRequest request = RegisterNodeRequest.newBuilder()
                .setAddress(address)
                .setCapacity(capacity)
                .build();
        nodeId = controlPlane.registerNode(request).getNodeId();
        log.info("Registered as node {} at {}", nodeId, address);
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}

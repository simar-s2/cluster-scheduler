package clusterscheduler.worker;

import clusterscheduler.proto.Resources;

/** One worker node: builds every component and owns their lifecycle. */
public final class WorkerNode implements AutoCloseable {
    private final ControlPlaneClient controlPlaneClient;
    private final HeartbeatLoop heartbeatLoop;

    private WorkerNode(ControlPlaneClient controlPlaneClient, HeartbeatLoop heartbeatLoop) {
        this.controlPlaneClient = controlPlaneClient;
        this.heartbeatLoop = heartbeatLoop;
    }

    public static WorkerNode start(WorkerConfig config) {
        Resources capacity = Resources.newBuilder()
                .setCpuMillis(config.cpuCapacityMillis())
                .setMemoryMb(config.memoryCapacityMb())
                .build();
        String address = config.advertisedHost() + ":" + config.grpcPort();

        ControlPlaneClient controlPlaneClient = new ControlPlaneClient(config.controlPlaneAddresses());
        HeartbeatLoop heartbeatLoop =
                new HeartbeatLoop(controlPlaneClient, address, capacity, config.heartbeatIntervalMillis());
        heartbeatLoop.start();
        return new WorkerNode(controlPlaneClient, heartbeatLoop);
    }

    /** The current node ID, or null while the worker is not registered. */
    public String nodeId() {
        return heartbeatLoop.nodeId();
    }

    /**
     * Stops everything without telling the control plane, exactly like a
     * crash. The control plane notices only because heartbeats stop.
     */
    @Override
    public void close() {
        heartbeatLoop.close();
        controlPlaneClient.close();
    }
}

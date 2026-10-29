package clusterscheduler.worker;

import clusterscheduler.proto.Resources;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.List;

/** One worker node: builds every component and owns their lifecycle. */
public final class WorkerNode implements AutoCloseable {
    private final Server grpcServer;
    private final TaskRunner taskRunner;
    private final ControlPlaneClient controlPlaneClient;
    private final HeartbeatLoop heartbeatLoop;

    private WorkerNode(
            Server grpcServer,
            TaskRunner taskRunner,
            ControlPlaneClient controlPlaneClient,
            HeartbeatLoop heartbeatLoop) {
        this.grpcServer = grpcServer;
        this.taskRunner = taskRunner;
        this.controlPlaneClient = controlPlaneClient;
        this.heartbeatLoop = heartbeatLoop;
    }

    public static WorkerNode start(WorkerConfig config) throws IOException {
        Resources capacity = Resources.newBuilder()
                .setCpuMillis(config.cpuCapacityMillis())
                .setMemoryMb(config.memoryCapacityMb())
                .build();
        TaskRunner taskRunner = new TaskRunner(config.cpuCapacityMillis(), config.memoryCapacityMb());
        ControlPlaneClient controlPlaneClient = new ControlPlaneClient(config.controlPlaneAddresses());

        // The service needs the node ID, which only exists after registering,
        // so it reads it through this holder.
        HeartbeatLoop[] heartbeatLoopHolder = new HeartbeatLoop[1];
        Server grpcServer = ServerBuilder.forPort(config.grpcPort())
                .addService(new WorkerService(taskRunner, () -> heartbeatLoopHolder[0].nodeId()))
                .build()
                .start();

        // Advertise the port actually bound, which matters when the config asked for port 0.
        String address = config.advertisedHost() + ":" + grpcServer.getPort();
        HeartbeatLoop heartbeatLoop = new HeartbeatLoop(
                controlPlaneClient, taskRunner, address, capacity, config.heartbeatIntervalMillis());
        heartbeatLoopHolder[0] = heartbeatLoop;
        heartbeatLoop.start();
        return new WorkerNode(grpcServer, taskRunner, controlPlaneClient, heartbeatLoop);
    }

    /** The current node ID, or null while the worker is not registered. */
    public String nodeId() {
        return heartbeatLoop.nodeId();
    }

    public List<String> runningAssignmentIds() {
        return taskRunner.runningAssignmentIds();
    }

    /**
     * Stops everything without telling the control plane, exactly like a
     * crash. The control plane notices only because heartbeats stop.
     */
    @Override
    public void close() {
        heartbeatLoop.close();
        grpcServer.shutdownNow();
        taskRunner.close();
        controlPlaneClient.close();
    }
}

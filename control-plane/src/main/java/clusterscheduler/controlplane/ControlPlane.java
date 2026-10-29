package clusterscheduler.controlplane;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.controlplane.database.SchemaInitializer;
import clusterscheduler.controlplane.delivery.AssignmentSender;
import clusterscheduler.controlplane.reconcile.ReconcileLoop;
import clusterscheduler.controlplane.scheduling.SchedulingLoop;
import clusterscheduler.controlplane.state.AssignmentRepository;
import clusterscheduler.controlplane.state.HeartbeatRepository;
import clusterscheduler.controlplane.state.JobRepository;
import clusterscheduler.controlplane.state.NodeRepository;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One control plane replica: builds every component and owns their lifecycle. */
public final class ControlPlane implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ControlPlane.class);

    private final Database database;
    private final Server grpcServer;
    private final AssignmentSender assignmentSender;
    // Every decision (placing tasks, declaring nodes dead) runs on this one
    // thread, so decisions never race each other inside a replica.
    private final ScheduledExecutorService decisionThread =
            Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "decisions"));

    private ControlPlane(Database database, Server grpcServer, AssignmentSender assignmentSender) {
        this.database = database;
        this.grpcServer = grpcServer;
        this.assignmentSender = assignmentSender;
    }

    public static ControlPlane start(ControlPlaneConfig config) throws IOException {
        Database database = new Database(config.databaseUrl(), config.databaseUser(), config.databasePassword());
        SchemaInitializer.apply(database);
        NodeRepository nodeRepository = new NodeRepository(database);
        HeartbeatRepository heartbeatRepository = new HeartbeatRepository(database);
        JobRepository jobRepository = new JobRepository(database);
        AssignmentRepository assignmentRepository = new AssignmentRepository();
        AssignmentSender assignmentSender = new AssignmentSender();

        Server grpcServer = ServerBuilder.forPort(config.grpcPort())
                .addService(new ControlPlaneService(nodeRepository, heartbeatRepository, jobRepository))
                .build()
                .start();
        ControlPlane controlPlane = new ControlPlane(database, grpcServer, assignmentSender);

        SchedulingLoop schedulingLoop = new SchedulingLoop(
                database, jobRepository, nodeRepository, assignmentRepository, assignmentSender,
                config.schedulingBatchSize());
        ReconcileLoop reconcileLoop = new ReconcileLoop(
                database, nodeRepository, assignmentRepository, assignmentSender,
                config.heartbeatTimeoutMillis(), config.resendAfterMillis());
        controlPlane.repeatOnDecisionThread("scheduling", schedulingLoop::runOnce, config.schedulingIntervalMillis());
        controlPlane.repeatOnDecisionThread("reconcile", reconcileLoop::runOnce, config.reconcileIntervalMillis());
        return controlPlane;
    }

    /** The port the gRPC server listens on. Useful when the config asked for port 0. */
    public int grpcPort() {
        return grpcServer.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    /**
     * Runs the step on a fixed delay. A failed run is logged and the next one
     * still happens; without the catch, the executor would silently cancel
     * the step after its first exception.
     */
    private void repeatOnDecisionThread(String stepName, Runnable step, long intervalMillis) {
        decisionThread.scheduleWithFixedDelay(() -> {
            try {
                step.run();
            } catch (RuntimeException failure) {
                log.warn("{} step failed: {}", stepName, failure.getMessage());
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        decisionThread.shutdownNow();
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(2, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            grpcServer.shutdownNow();
            Thread.currentThread().interrupt();
        }
        assignmentSender.close();
        database.close();
    }
}

package clusterscheduler.controlplane;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.controlplane.database.SchemaInitializer;
import clusterscheduler.controlplane.delivery.AssignmentSender;
import clusterscheduler.controlplane.leadership.FencedTransactions;
import clusterscheduler.controlplane.leadership.LeaderElector;
import clusterscheduler.controlplane.leadership.Leadership;
import clusterscheduler.controlplane.leadership.LeaseRepository;
import clusterscheduler.controlplane.leadership.StaleEpochException;
import clusterscheduler.controlplane.reconcile.ReconcileLoop;
import clusterscheduler.controlplane.scheduling.SchedulingLoop;
import clusterscheduler.controlplane.state.AssignmentRepository;
import clusterscheduler.controlplane.state.HeartbeatRepository;
import clusterscheduler.controlplane.state.JobRepository;
import clusterscheduler.controlplane.state.NodeRepository;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One control plane replica: builds every component and owns their lifecycle. */
public final class ControlPlane implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ControlPlane.class);

    private final Database database;
    private final Server grpcServer;
    private final AssignmentSender assignmentSender;
    private final Leadership leadership;
    private final LeaderElector leaderElector;
    // Every decision (placing tasks, declaring nodes dead) runs on this one
    // thread, so decisions never race each other inside a replica.
    private final ScheduledExecutorService decisionThread =
            Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "decisions"));

    private ControlPlane(
            Database database,
            Server grpcServer,
            AssignmentSender assignmentSender,
            Leadership leadership,
            LeaderElector leaderElector) {
        this.database = database;
        this.grpcServer = grpcServer;
        this.assignmentSender = assignmentSender;
        this.leadership = leadership;
        this.leaderElector = leaderElector;
    }

    public static ControlPlane start(ControlPlaneConfig config) throws IOException {
        Database database = new Database(config.databaseUrl(), config.databaseUser(), config.databasePassword());
        SchemaInitializer.apply(database);
        NodeRepository nodeRepository = new NodeRepository(database);
        HeartbeatRepository heartbeatRepository = new HeartbeatRepository(database);
        JobRepository jobRepository = new JobRepository(database);
        AssignmentRepository assignmentRepository = new AssignmentRepository();
        Leadership leadership = new Leadership();
        AssignmentSender assignmentSender = new AssignmentSender(leadership);

        Server grpcServer = NettyServerBuilder
                .forAddress(new InetSocketAddress(config.grpcBindHost(), config.grpcPort()))
                .addService(new ControlPlaneService(nodeRepository, heartbeatRepository, jobRepository))
                .build()
                .start();
        LeaderElector leaderElector = new LeaderElector(
                config.replicaId(), new LeaseRepository(database), leadership,
                config.leaseDurationMillis(), config.leaseRenewIntervalMillis());
        ControlPlane controlPlane =
                new ControlPlane(database, grpcServer, assignmentSender, leadership, leaderElector);

        FencedTransactions fencedTransactions = new FencedTransactions(database);
        SchedulingLoop schedulingLoop = new SchedulingLoop(
                fencedTransactions, jobRepository, nodeRepository, assignmentRepository, assignmentSender,
                config.schedulingBatchSize());
        ReconcileLoop reconcileLoop = new ReconcileLoop(
                database, fencedTransactions, nodeRepository, assignmentRepository, assignmentSender,
                config.heartbeatTimeoutMillis(), config.resendAfterMillis());
        controlPlane.repeatWhileLeader("scheduling", schedulingLoop::runOnce, config.schedulingIntervalMillis());
        controlPlane.repeatWhileLeader("reconcile", reconcileLoop::runOnce, config.reconcileIntervalMillis());
        leaderElector.start();
        return controlPlane;
    }

    /** The port the gRPC server listens on. Useful when the config asked for port 0. */
    public int grpcPort() {
        return grpcServer.getPort();
    }

    public boolean isLeader() {
        return leadership.isLeader();
    }

    /** The epoch this replica leads under, or empty if it is a standby. */
    public OptionalLong currentEpoch() {
        return leadership.currentEpoch();
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    /**
     * Runs the step on a fixed delay, but only while this replica is the
     * leader, and passes it the current epoch. A failed run is logged and the
     * next one still happens; without the catch, the executor would silently
     * cancel the step after its first exception.
     */
    private void repeatWhileLeader(String stepName, LongConsumer step, long intervalMillis) {
        decisionThread.scheduleWithFixedDelay(() -> {
            OptionalLong epoch = leadership.currentEpoch();
            if (epoch.isEmpty()) {
                return;
            }
            try {
                step.accept(epoch.getAsLong());
            } catch (StaleEpochException fencedOut) {
                // PostgreSQL refused the write: a newer leader exists. Stop
                // acting at once instead of waiting for the local deadline.
                log.warn("FENCED: {} write rejected, {}; stepping down", stepName, fencedOut.getMessage());
                leadership.stepDown(fencedOut.staleEpoch());
            } catch (RuntimeException failure) {
                log.warn("{} step failed: {}", stepName, failure.getMessage());
            }
        }, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        leaderElector.close();
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

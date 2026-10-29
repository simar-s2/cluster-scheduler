package clusterscheduler.controlplane;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.proto.ControlPlaneGrpc;
import clusterscheduler.proto.Resources;
import clusterscheduler.proto.SubmitJobRequest;
import clusterscheduler.worker.WorkerConfig;
import clusterscheduler.worker.WorkerNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Real control plane replicas and real workers, all in this JVM, talking
 * gRPC over localhost and sharing the test PostgreSQL.
 */
final class TestCluster implements AutoCloseable {
    static final long WORKER_CPU_MILLIS = 4_000;
    static final long WORKER_MEMORY_MB = 8_192;

    private final List<ControlPlane> controlPlanes = new ArrayList<>();
    private final List<Boolean> controlPlaneRunning = new ArrayList<>();
    private final List<WorkerNode> workers = new ArrayList<>();
    private final Database database = TestDatabase.connect();

    static TestCluster start(int replicaCount, int workerCount, Map<String, String> controlPlaneOverrides) {
        TestDatabase.reset();
        TestCluster cluster = new TestCluster();
        try {
            for (int i = 0; i < replicaCount; i++) {
                cluster.controlPlanes.add(ControlPlane.start(
                        TestDatabase.configForReplica("replica-" + i, controlPlaneOverrides)));
                cluster.controlPlaneRunning.add(true);
            }
            for (int i = 0; i < workerCount; i++) {
                cluster.addWorker();
            }
        } catch (IOException failure) {
            cluster.close();
            throw new UncheckedIOException(failure);
        }
        return cluster;
    }

    WorkerNode addWorker() throws IOException {
        return addWorker(100);
    }

    WorkerNode addWorker(long heartbeatIntervalMillis) throws IOException {
        List<String> replicaAddresses = controlPlanes.stream()
                .map(controlPlane -> "localhost:" + controlPlane.grpcPort())
                .toList();
        WorkerNode worker = WorkerNode.start(new WorkerConfig(
                replicaAddresses, "127.0.0.1", 0, WORKER_CPU_MILLIS, WORKER_MEMORY_MB, heartbeatIntervalMillis));
        workers.add(worker);
        return worker;
    }

    ControlPlane controlPlane(int index) {
        return controlPlanes.get(index);
    }

    WorkerNode worker(int index) {
        return workers.get(index);
    }

    List<WorkerNode> workers() {
        return workers;
    }

    /** Stops a replica the way a crash would: no lease release, no goodbye. */
    void crashControlPlane(int index) {
        controlPlanes.get(index).close();
        controlPlaneRunning.set(index, false);
    }

    /** Stops a worker the way a crash would. */
    void crashWorker(int index) {
        workers.get(index).close();
    }

    void waitUntilAllWorkersRegistered() {
        Eventually.waitUntil("every worker is registered", Duration.ofSeconds(10),
                () -> workers.stream().allMatch(worker -> worker.nodeId() != null));
    }

    /** Submits through the first running replica. */
    void submitJob(String jobId, long cpuMillis, long memoryMb, long durationMillis) {
        SubmitJobRequest request = SubmitJobRequest.newBuilder()
                .setJobId(jobId)
                .setRequested(Resources.newBuilder().setCpuMillis(cpuMillis).setMemoryMb(memoryMb))
                .setDurationMillis(durationMillis)
                .build();
        for (int i = 0; i < controlPlanes.size(); i++) {
            if (!controlPlaneRunning.get(i)) {
                continue;
            }
            ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:" + controlPlanes.get(i).grpcPort())
                    .usePlaintext()
                    .build();
            try {
                ControlPlaneGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(2, TimeUnit.SECONDS)
                        .submitJob(request);
                return;
            } catch (StatusRuntimeException failure) {
                // Try the next replica.
            } finally {
                channel.shutdownNow();
            }
        }
        throw new AssertionError("No replica accepted job " + jobId);
    }

    /** Runs any write, for tests that set up a state directly. */
    void execute(String sql, Object... parameters) {
        database.inTransaction(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (int i = 0; i < parameters.length; i++) {
                    statement.setObject(i + 1, parameters[i]);
                }
                return statement.executeUpdate();
            }
        });
    }

    /** Runs a query that returns one number, such as SELECT count(*). */
    long queryNumber(String sql, Object... parameters) {
        return database.inTransaction(connection -> {
            try (PreparedStatement query = connection.prepareStatement(sql)) {
                for (int i = 0; i < parameters.length; i++) {
                    query.setObject(i + 1, parameters[i]);
                }
                try (ResultSet row = query.executeQuery()) {
                    row.next();
                    return row.getLong(1);
                }
            }
        });
    }

    /** CPU held by active assignments, per node. */
    Map<String, Long> allocatedCpuByNode() {
        return database.inTransaction(connection -> {
            String sql = """
                    SELECT a.node_id, SUM(j.cpu_millis)
                    FROM assignments a JOIN jobs j ON j.job_id = a.job_id
                    WHERE a.state IN ('ASSIGNED', 'RUNNING')
                    GROUP BY a.node_id
                    """;
            try (PreparedStatement query = connection.prepareStatement(sql);
                    ResultSet rows = query.executeQuery()) {
                Map<String, Long> cpuByNode = new HashMap<>();
                while (rows.next()) {
                    cpuByNode.put(rows.getString(1), rows.getLong(2));
                }
                return cpuByNode;
            }
        });
    }

    @Override
    public void close() {
        workers.forEach(WorkerNode::close);
        for (int i = 0; i < controlPlanes.size(); i++) {
            if (controlPlaneRunning.get(i)) {
                controlPlanes.get(i).close();
            }
        }
        database.close();
    }
}

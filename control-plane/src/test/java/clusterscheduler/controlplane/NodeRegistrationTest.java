package clusterscheduler.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.worker.WorkerConfig;
import clusterscheduler.worker.WorkerNode;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NodeRegistrationTest {
    private ControlPlane controlPlane;
    private WorkerNode worker;
    private Database database;

    @BeforeEach
    void startControlPlane() throws Exception {
        TestDatabase.reset();
        database = TestDatabase.connect();
        controlPlane = ControlPlane.start(TestDatabase.configForReplica("replica-1"));
        worker = WorkerNode.start(new WorkerConfig(
                List.of("localhost:" + controlPlane.grpcPort()), "127.0.0.1", 0, 4_000, 8_192, 100));
    }

    @AfterEach
    void stopEverything() {
        worker.close();
        controlPlane.close();
        database.close();
    }

    @Test
    void workerRegistersWithItsCapacity() {
        Eventually.waitUntil("the worker is registered", Duration.ofSeconds(10), () -> worker.nodeId() != null);

        long[] capacity = database.inTransaction(connection -> {
            String sql = "SELECT cpu_capacity_millis, memory_capacity_mb FROM nodes WHERE node_id = ?";
            try (PreparedStatement query = connection.prepareStatement(sql)) {
                query.setString(1, worker.nodeId());
                try (ResultSet row = query.executeQuery()) {
                    row.next();
                    return new long[] {row.getLong(1), row.getLong(2)};
                }
            }
        });
        assertEquals(4_000, capacity[0]);
        assertEquals(8_192, capacity[1]);
    }

    @Test
    void heartbeatsMoveTheLastHeartbeatTimeForward() {
        Eventually.waitUntil("the worker is registered", Duration.ofSeconds(10), () -> worker.nodeId() != null);
        Timestamp first = lastHeartbeatAt(worker.nodeId());

        Eventually.waitUntil("a later heartbeat is recorded", Duration.ofSeconds(5),
                () -> lastHeartbeatAt(worker.nodeId()).after(first));
    }

    @Test
    void workerRegistersAgainWithANewIdWhenTheControlPlaneForgetsIt() {
        Eventually.waitUntil("the worker is registered", Duration.ofSeconds(10), () -> worker.nodeId() != null);
        String firstNodeId = worker.nodeId();

        deleteNode(firstNodeId);

        Eventually.waitUntil("the worker registers again", Duration.ofSeconds(5),
                () -> worker.nodeId() != null && !worker.nodeId().equals(firstNodeId));
        assertNotEquals(firstNodeId, worker.nodeId());
    }

    private Timestamp lastHeartbeatAt(String nodeId) {
        return database.inTransaction(connection -> {
            try (PreparedStatement query =
                    connection.prepareStatement("SELECT last_heartbeat_at FROM nodes WHERE node_id = ?")) {
                query.setString(1, nodeId);
                try (ResultSet row = query.executeQuery()) {
                    row.next();
                    return row.getTimestamp(1);
                }
            }
        });
    }

    private void deleteNode(String nodeId) {
        database.inTransaction(connection -> {
            try (PreparedStatement delete = connection.prepareStatement("DELETE FROM nodes WHERE node_id = ?")) {
                delete.setString(1, nodeId);
                return delete.executeUpdate();
            }
        });
    }
}

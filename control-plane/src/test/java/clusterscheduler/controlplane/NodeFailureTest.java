package clusterscheduler.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import clusterscheduler.worker.WorkerNode;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class NodeFailureTest {
    // Short timeouts so failures are noticed within about a second.
    private static final Map<String, String> FAST_FAILURE_DETECTION = Map.of(
            "HEARTBEAT_TIMEOUT_MILLIS", "700",
            "RECONCILE_INTERVAL_MILLIS", "100",
            "RESEND_AFTER_MILLIS", "500");

    private TestCluster cluster;

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    void tasksOfACrashedWorkerMoveToHealthyNodes() {
        cluster = TestCluster.start(1, 3, FAST_FAILURE_DETECTION);
        cluster.waitUntilAllWorkersRegistered();
        for (int i = 0; i < 6; i++) {
            cluster.submitJob("job-" + i, 1_000, 1_000, 600_000);
        }
        waitUntilRunning(6);
        int victimIndex = indexOfBusiestWorker();
        String victimNodeId = cluster.worker(victimIndex).nodeId();
        long victimTasks = cluster.worker(victimIndex).runningAssignmentIds().size();

        cluster.crashWorker(victimIndex);

        Eventually.waitUntil("the crashed node is declared dead", Duration.ofSeconds(5),
                () -> cluster.queryNumber("SELECT count(*) FROM nodes WHERE node_id = ? AND status = 'DEAD'",
                        victimNodeId) == 1);
        waitUntilRunning(6);
        assertEquals(victimTasks,
                cluster.queryNumber("SELECT count(*) FROM assignments WHERE node_id = ? AND state = 'LOST'",
                        victimNodeId));
        assertEquals(0, cluster.queryNumber(
                "SELECT count(*) FROM assignments WHERE node_id = ? AND state IN ('ASSIGNED', 'RUNNING')",
                victimNodeId));
        assertEachJobHasOneActiveAssignment();
    }

    @Test
    void aNodeDeclaredDeadStopsItsTasksAndRegistersAsANewNode() throws IOException {
        cluster = TestCluster.start(1, 0, FAST_FAILURE_DETECTION);
        // This worker's heartbeats arrive less often than the timeout allows,
        // like a node on the far side of a slow network. Every time it reports
        // in, it has already been declared dead.
        WorkerNode lateWorker = cluster.addWorker(1_500);
        cluster.waitUntilAllWorkersRegistered();
        String firstNodeId = lateWorker.nodeId();
        // The late worker is the only node, so the job has to go there.
        cluster.submitJob("job", 1_000, 1_000, 600_000);
        Eventually.waitUntil("the job runs on the late worker", Duration.ofSeconds(5),
                () -> !lateWorker.runningAssignmentIds().isEmpty());
        String firstAssignmentId = lateWorker.runningAssignmentIds().get(0);
        WorkerNode healthyWorker = cluster.addWorker();

        Eventually.waitUntil("the job moves to the healthy worker", Duration.ofSeconds(10),
                () -> healthyWorker.runningAssignmentIds().size() == 1);
        Eventually.waitUntil("the late worker registers again", Duration.ofSeconds(10),
                () -> lateWorker.nodeId() != null && !lateWorker.nodeId().equals(firstNodeId));

        assertEquals(1, cluster.queryNumber("SELECT count(*) FROM nodes WHERE node_id = ? AND status = 'DEAD'",
                firstNodeId));
        assertEquals(1, cluster.queryNumber("SELECT count(*) FROM assignments WHERE assignment_id = ? AND state = 'LOST'",
                firstAssignmentId));
        assertFalse(lateWorker.runningAssignmentIds().contains(firstAssignmentId));
        assertEachJobHasOneActiveAssignment();
    }

    @Test
    void anAssignmentSavedButNeverSentIsResent() {
        cluster = TestCluster.start(1, 1, FAST_FAILURE_DETECTION);
        cluster.waitUntilAllWorkersRegistered();
        String nodeId = cluster.worker(0).nodeId();
        // What a leader leaves behind if it dies right after saving: the
        // assignment exists, but the worker never heard about it.
        cluster.execute("INSERT INTO jobs (job_id, cpu_millis, memory_mb, duration_millis, state) "
                + "VALUES ('orphan', 500, 500, 600000, 'SCHEDULED')");
        cluster.execute("INSERT INTO assignments (assignment_id, job_id, node_id, epoch) VALUES ('orphan-assignment', 'orphan', ?, 1)",
                nodeId);

        Eventually.waitUntil("the worker runs the resent assignment", Duration.ofSeconds(5),
                () -> cluster.worker(0).runningAssignmentIds().contains("orphan-assignment"));
        Eventually.waitUntil("a heartbeat confirms it", Duration.ofSeconds(5),
                () -> cluster.queryNumber("SELECT count(*) FROM assignments WHERE assignment_id = 'orphan-assignment' "
                        + "AND state = 'RUNNING'") == 1);
        assertFalse(cluster.worker(0).runningAssignmentIds().isEmpty());
    }

    private void waitUntilRunning(int expected) {
        Eventually.waitUntil(expected + " assignments are confirmed running", Duration.ofSeconds(10),
                () -> cluster.queryNumber("SELECT count(*) FROM assignments WHERE state = 'RUNNING'") == expected);
    }

    private int indexOfBusiestWorker() {
        int busiest = 0;
        for (int i = 1; i < cluster.workers().size(); i++) {
            if (cluster.worker(i).runningAssignmentIds().size() > cluster.worker(busiest).runningAssignmentIds().size()) {
                busiest = i;
            }
        }
        return busiest;
    }

    private void assertEachJobHasOneActiveAssignment() {
        assertEquals(0, cluster.queryNumber("""
                SELECT count(*) FROM (
                    SELECT job_id FROM assignments
                    WHERE state IN ('ASSIGNED', 'RUNNING')
                    GROUP BY job_id HAVING count(*) > 1
                ) AS doubled
                """));
    }
}

package clusterscheduler.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import clusterscheduler.proto.AssignTaskRequest;
import clusterscheduler.proto.AssignTaskResponse;
import clusterscheduler.proto.Resources;
import clusterscheduler.proto.WorkerGrpc;
import clusterscheduler.worker.WorkerNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeaderFailoverTest {
    private TestCluster cluster;

    @BeforeEach
    void startThreeReplicas() {
        cluster = TestCluster.start(3, 3, Map.of());
        cluster.waitUntilAllWorkersRegistered();
        waitForALeader();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    void neverMoreThanOneLeaderAtATime() throws InterruptedException {
        for (int sample = 0; sample < 40; sample++) {
            assertTrue(leaderCount() <= 1, "more than one replica thinks it leads");
            Thread.sleep(50);
        }
        assertEquals(1, leaderCount());
    }

    @Test
    void aStandbyTakesOverWithAHigherEpochWhenTheLeaderCrashes() {
        int oldLeader = leaderIndex();
        long oldEpoch = cluster.controlPlane(oldLeader).currentEpoch().getAsLong();

        cluster.crashControlPlane(oldLeader);

        Eventually.waitUntil("a standby becomes leader", Duration.ofSeconds(10),
                () -> leaderIndexAmongRunning(oldLeader) >= 0);
        int newLeader = leaderIndexAmongRunning(oldLeader);
        long newEpoch = cluster.controlPlane(newLeader).currentEpoch().getAsLong();
        assertTrue(newEpoch > oldEpoch, "new epoch " + newEpoch + " is not above " + oldEpoch);
        assertEquals(newEpoch, cluster.queryNumber("SELECT epoch FROM leader_lease"));
    }

    @Test
    void schedulingContinuesAfterFailoverWithoutPlacingAnyJobTwice() {
        for (int i = 0; i < 6; i++) {
            cluster.submitJob("before-" + i, 500, 500, 600_000);
        }
        Eventually.waitUntil("the first jobs are scheduled", Duration.ofSeconds(10),
                () -> cluster.queryNumber("SELECT count(*) FROM jobs WHERE state = 'SCHEDULED'") == 6);

        int oldLeader = leaderIndex();
        cluster.crashControlPlane(oldLeader);
        for (int i = 0; i < 6; i++) {
            cluster.submitJob("after-" + i, 500, 500, 600_000);
        }

        Eventually.waitUntil("the new leader schedules the rest", Duration.ofSeconds(10),
                () -> cluster.queryNumber("SELECT count(*) FROM jobs WHERE state = 'SCHEDULED'") == 12);
        assertEquals(12, cluster.queryNumber("SELECT count(*) FROM assignments"));
        assertEquals(0, cluster.queryNumber(
                "SELECT count(*) FROM (SELECT job_id FROM assignments GROUP BY job_id HAVING count(*) > 1) AS doubled"));
    }

    @Test
    void standbysKeepRecordingHeartbeatsWhileThereIsNoLeader() throws InterruptedException {
        int oldLeader = leaderIndex();
        cluster.crashControlPlane(oldLeader);

        // Until the lease expires there is no leader at all. Heartbeats must
        // keep landing anyway, or the new leader would find every node
        // overdue and declare healthy nodes dead.
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (leaderIndexAmongRunning(oldLeader) < 0 && System.nanoTime() < deadline) {
            assertEquals(0, cluster.queryNumber(
                    "SELECT count(*) FROM nodes WHERE last_heartbeat_at < now() - interval '1 second'"));
            Thread.sleep(100);
        }
        assertTrue(leaderIndexAmongRunning(oldLeader) >= 0, "no new leader within 10 seconds");
        assertEquals(0, cluster.queryNumber("SELECT count(*) FROM nodes WHERE status = 'DEAD'"));
    }

    @Test
    void aWorkerRejectsAssignmentsFromTheReplacedLeader() {
        int oldLeader = leaderIndex();
        long oldEpoch = cluster.controlPlane(oldLeader).currentEpoch().getAsLong();
        cluster.crashControlPlane(oldLeader);
        Eventually.waitUntil("a new leader is elected", Duration.ofSeconds(10),
                () -> leaderIndexAmongRunning(oldLeader) >= 0);
        // Give every worker an assignment from the new leader, so each one has seen the new epoch.
        for (int i = 0; i < 3; i++) {
            cluster.submitJob("from-new-leader-" + i, 3_000, 1_000, 600_000);
        }
        Eventually.waitUntil("each worker runs a job from the new leader", Duration.ofSeconds(10),
                () -> cluster.workers().stream().allMatch(worker -> worker.runningAssignmentIds().size() == 1));

        // What the old leader would send if it had been frozen rather than killed.
        WorkerNode worker = cluster.worker(0);
        String address = cluster.queryText("SELECT address FROM nodes WHERE node_id = ?", worker.nodeId());
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        try {
            AssignTaskResponse response = WorkerGrpc.newBlockingStub(channel).assignTask(AssignTaskRequest.newBuilder()
                    .setAssignmentId("from-old-leader")
                    .setJobId("from-old-leader")
                    .setNodeId(worker.nodeId())
                    .setLeaderEpoch(oldEpoch)
                    .setRequested(Resources.newBuilder().setCpuMillis(100).setMemoryMb(100))
                    .setDurationMillis(600_000)
                    .build());
            assertEquals(AssignTaskResponse.Result.RESULT_STALE_EPOCH, response.getResult());
        } finally {
            channel.shutdownNow();
        }
        assertFalse(worker.runningAssignmentIds().contains("from-old-leader"));
    }

    private void waitForALeader() {
        Eventually.waitUntil("a leader is elected", Duration.ofSeconds(10), () -> leaderCount() == 1);
    }

    private long leaderCount() {
        return IntStream.range(0, 3).filter(i -> cluster.controlPlane(i).isLeader()).count();
    }

    private int leaderIndex() {
        return IntStream.range(0, 3).filter(i -> cluster.controlPlane(i).isLeader()).findFirst().orElseThrow();
    }

    private int leaderIndexAmongRunning(int crashedIndex) {
        return IntStream.range(0, 3)
                .filter(i -> i != crashedIndex && cluster.controlPlane(i).isLeader())
                .findFirst()
                .orElse(-1);
    }
}

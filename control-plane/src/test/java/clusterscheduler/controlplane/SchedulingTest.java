package clusterscheduler.controlplane;

import static org.junit.jupiter.api.Assertions.assertEquals;

import clusterscheduler.worker.WorkerNode;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SchedulingTest {
    private TestCluster cluster;

    @BeforeEach
    void startCluster() {
        cluster = TestCluster.start(1, 3, Map.of());
        cluster.waitUntilAllWorkersRegistered();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    void fillsEveryNodeExactlyWithoutOvercommitting() {
        // 12 jobs of 1000m on 3 nodes of 4000m: a perfect fit, 4 per node.
        for (int i = 0; i < 12; i++) {
            cluster.submitJob("job-" + i, 1_000, 1_000, 60_000);
        }

        Eventually.waitUntil("all 12 jobs run on workers", Duration.ofSeconds(10), () ->
                cluster.workers().stream().mapToInt(worker -> worker.runningAssignmentIds().size()).sum() == 12);
        Eventually.waitUntil("heartbeats confirm all 12 as running", Duration.ofSeconds(5),
                () -> cluster.queryNumber("SELECT count(*) FROM assignments WHERE state = 'RUNNING'") == 12);
        cluster.allocatedCpuByNode().values().forEach(cpu -> assertEquals(4_000L, cpu));
        for (WorkerNode worker : cluster.workers()) {
            assertEquals(4, worker.runningAssignmentIds().size());
        }
    }

    @Test
    void finishedJobsFreeTheirSpaceForMoreJobs() {
        // Twice the cluster's CPU: the second half only fits after the first finishes.
        for (int i = 0; i < 12; i++) {
            cluster.submitJob("job-" + i, 2_000, 1_000, 300);
        }

        Eventually.waitUntil("every job finishes", Duration.ofSeconds(15),
                () -> cluster.queryNumber("SELECT count(*) FROM jobs WHERE state = 'FINISHED'") == 12);
        assertEquals(12, cluster.queryNumber("SELECT count(*) FROM assignments WHERE state = 'FINISHED'"));
    }

    @Test
    void aJobThatFitsNowhereWaitsWithoutBlockingSmallerJobs() throws InterruptedException {
        cluster.submitJob("too-big", TestCluster.WORKER_CPU_MILLIS + 1, 1_000, 1_000);
        cluster.submitJob("small", 500, 500, 60_000);

        Eventually.waitUntil("the small job is scheduled", Duration.ofSeconds(10),
                () -> cluster.queryNumber("SELECT count(*) FROM jobs WHERE job_id = 'small' AND state = 'SCHEDULED'") == 1);
        Thread.sleep(500);
        assertEquals(1, cluster.queryNumber("SELECT count(*) FROM jobs WHERE job_id = 'too-big' AND state = 'PENDING'"));
    }

    @Test
    void submittingTheSameJobIdTwiceCreatesOneJob() {
        cluster.submitJob("retried", 500, 500, 60_000);
        cluster.submitJob("retried", 500, 500, 60_000);

        Eventually.waitUntil("the job is scheduled", Duration.ofSeconds(10),
                () -> cluster.queryNumber("SELECT count(*) FROM assignments WHERE job_id = 'retried'") == 1);
        assertEquals(1, cluster.queryNumber("SELECT count(*) FROM jobs WHERE job_id = 'retried'"));
    }
}

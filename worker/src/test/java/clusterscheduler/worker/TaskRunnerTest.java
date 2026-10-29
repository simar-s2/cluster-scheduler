package clusterscheduler.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TaskRunnerTest {
    private final TaskRunner taskRunner = new TaskRunner(4_000, 8_192);

    @AfterEach
    void stopTimer() {
        taskRunner.close();
    }

    @Test
    void runningTasksHoldTheirResources() {
        taskRunner.startIfNew("a1", 1_000, 2_000, 60_000);
        taskRunner.startIfNew("a2", 500, 1_000, 60_000);

        TaskRunner.Snapshot snapshot = taskRunner.snapshot();
        assertEquals(2_500, snapshot.freeCpuMillis());
        assertEquals(5_192, snapshot.freeMemoryMb());
        assertEquals(2, snapshot.runningAssignmentIds().size());
    }

    @Test
    void theSameAssignmentStartsOnlyOnce() {
        assertTrue(taskRunner.startIfNew("a1", 1_000, 2_000, 60_000));
        assertFalse(taskRunner.startIfNew("a1", 1_000, 2_000, 60_000));

        assertEquals(3_000, taskRunner.snapshot().freeCpuMillis());
    }

    @Test
    void aFinishedAssignmentIsStillADuplicate() throws InterruptedException {
        taskRunner.startIfNew("a1", 1_000, 2_000, 0);
        waitUntilFinished("a1");

        assertFalse(taskRunner.startIfNew("a1", 1_000, 2_000, 0));
    }

    @Test
    void finishedTasksAreReportedUntilAcknowledged() throws InterruptedException {
        taskRunner.startIfNew("a1", 1_000, 2_000, 0);
        waitUntilFinished("a1");

        assertEquals(List.of("a1"), taskRunner.snapshot().finishedAssignmentIds());
        assertEquals(List.of("a1"), taskRunner.snapshot().finishedAssignmentIds());

        taskRunner.markFinishedReported(List.of("a1"));
        assertEquals(List.of(), taskRunner.snapshot().finishedAssignmentIds());
        assertEquals(4_000, taskRunner.snapshot().freeCpuMillis());
    }

    private void waitUntilFinished(String assignmentId) throws InterruptedException {
        for (int attempt = 0; attempt < 200; attempt++) {
            if (taskRunner.snapshot().finishedAssignmentIds().contains(assignmentId)) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError(assignmentId + " never finished");
    }

    @Test
    void stopAllDropsRunningTasksButStillRemembersTheirIds() {
        taskRunner.startIfNew("a1", 1_000, 2_000, 60_000);

        taskRunner.stopAll();

        assertEquals(List.of(), taskRunner.snapshot().runningAssignmentIds());
        assertEquals(4_000, taskRunner.snapshot().freeCpuMillis());
        assertFalse(taskRunner.startIfNew("a1", 1_000, 2_000, 60_000));
    }
}

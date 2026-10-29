package clusterscheduler.experiments;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

/** The checker must be able to see a double placement, or a result of zero would mean nothing. */
class WorkerLogCheckTest {

    @Test
    void aCleanRunHasNoFindings() {
        WorkerLogCheck.Findings findings = WorkerLogCheck.check(List.of(
                event("started", "node-a", "a1", "job-1", 100),
                event("finished", "node-a", "a1", "job-1", 200),
                event("started", "node-b", "a2", "job-2", 150),
                event("finished", "node-b", "a2", "job-2", 250)));

        assertEquals(new WorkerLogCheck.Findings(2, 0, 0, 0, 0), findings);
    }

    @Test
    void detectsAJobRunningOnTwoNodesAtOnce() {
        WorkerLogCheck.Findings findings = WorkerLogCheck.check(List.of(
                event("started", "node-a", "a1", "job-1", 100),
                event("started", "node-b", "a2", "job-1", 150),
                event("finished", "node-a", "a1", "job-1", 200),
                event("finished", "node-b", "a2", "job-1", 250)));

        assertEquals(1, findings.jobsRunOnTwoNodesAtOnce());
        assertEquals(1, findings.jobsStartedOnMoreThanOneNode());
    }

    @Test
    void aJobMovedAfterItsFirstRunStoppedIsNotARunAtOnce() {
        WorkerLogCheck.Findings findings = WorkerLogCheck.check(List.of(
                event("started", "node-a", "a1", "job-1", 100),
                event("stopped", "node-a", "a1", "job-1", 200),
                event("started", "node-b", "a2", "job-1", 300),
                event("finished", "node-b", "a2", "job-1", 400)));

        assertEquals(0, findings.jobsRunOnTwoNodesAtOnce());
        assertEquals(1, findings.jobsStartedOnMoreThanOneNode());
    }

    @Test
    void aRunThatNeverEndedCountsAsStillRunning() {
        WorkerLogCheck.Findings findings = WorkerLogCheck.check(List.of(
                event("started", "node-a", "a1", "job-1", 100),
                event("started", "node-b", "a2", "job-1", 5_000)));

        assertEquals(1, findings.jobsRunOnTwoNodesAtOnce());
    }

    @Test
    void countsDuplicateStartsAndStaleEpochRejections() {
        WorkerLogCheck.Findings findings = WorkerLogCheck.check(List.of(
                event("started", "node-a", "a1", "job-1", 100),
                event("started", "node-a", "a1", "job-1", 120),
                event("rejected_stale_epoch", "node-a", "a9", "job-9", 130)));

        assertEquals(1, findings.assignmentsStartedTwice());
        assertEquals(1, findings.staleEpochRejections());
    }

    private static ExecutionEvent event(String type, String node, String assignment, String job, long time) {
        return new ExecutionEvent(type, node, assignment, job, 1, time);
    }
}

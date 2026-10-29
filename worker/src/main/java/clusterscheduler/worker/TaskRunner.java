package clusterscheduler.worker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Runs simulated tasks. A task does no real work: it holds its CPU and memory
 * for its duration and then finishes. That is enough to exercise scheduling
 * and failure handling, and lets one laptop run thousands of tasks.
 *
 * Every method is synchronized. The state is small and the calls are quick,
 * so one lock keeps the logic easy to check.
 */
final class TaskRunner implements AutoCloseable {
    private final long cpuCapacityMillis;
    private final long memoryCapacityMb;
    private final ScheduledExecutorService finishTimer =
            Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "task-timer"));

    private final Map<String, RunningTask> runningTasks = new HashMap<>();
    // Every assignment ID this worker has ever accepted. This is what makes a
    // repeated AssignTask harmless, even after the task has finished.
    private final Set<String> seenAssignmentIds = new HashSet<>();
    // Finished tasks the control plane has not been told about yet.
    private final List<String> unreportedFinishedIds = new ArrayList<>();

    TaskRunner(long cpuCapacityMillis, long memoryCapacityMb) {
        this.cpuCapacityMillis = cpuCapacityMillis;
        this.memoryCapacityMb = memoryCapacityMb;
    }

    /** Starts the task unless this assignment ID was seen before. Returns true if it started. */
    synchronized boolean startIfNew(String assignmentId, long cpuMillis, long memoryMb, long durationMillis) {
        if (!seenAssignmentIds.add(assignmentId)) {
            return false;
        }
        ScheduledFuture<?> finish =
                finishTimer.schedule(() -> finish(assignmentId), durationMillis, TimeUnit.MILLISECONDS);
        runningTasks.put(assignmentId, new RunningTask(cpuMillis, memoryMb, finish));
        return true;
    }

    /** What the next heartbeat should report. */
    synchronized Snapshot snapshot() {
        long usedCpuMillis = 0;
        long usedMemoryMb = 0;
        for (RunningTask task : runningTasks.values()) {
            usedCpuMillis += task.cpuMillis();
            usedMemoryMb += task.memoryMb();
        }
        return new Snapshot(
                cpuCapacityMillis - usedCpuMillis,
                memoryCapacityMb - usedMemoryMb,
                List.copyOf(runningTasks.keySet()),
                List.copyOf(unreportedFinishedIds));
    }

    /** Forgets finished tasks once a heartbeat carrying them has succeeded. */
    synchronized void markFinishedReported(List<String> reportedIds) {
        unreportedFinishedIds.removeAll(reportedIds);
    }

    /**
     * Stops every task. Used when the control plane says this node was
     * declared dead: its tasks have already been given to other nodes, and
     * running them here as well would run them twice.
     */
    synchronized void stopAll() {
        runningTasks.values().forEach(task -> task.finish().cancel(false));
        runningTasks.clear();
        // Those assignments are LOST on the control plane side, so there is no
        // point reporting them as finished.
        unreportedFinishedIds.clear();
    }

    synchronized List<String> runningAssignmentIds() {
        return List.copyOf(runningTasks.keySet());
    }

    private synchronized void finish(String assignmentId) {
        if (runningTasks.remove(assignmentId) != null) {
            unreportedFinishedIds.add(assignmentId);
        }
    }

    @Override
    public void close() {
        finishTimer.shutdownNow();
    }

    /** Free space and task lists at one moment, for a heartbeat. */
    record Snapshot(
            long freeCpuMillis,
            long freeMemoryMb,
            List<String> runningAssignmentIds,
            List<String> finishedAssignmentIds) {}

    private record RunningTask(long cpuMillis, long memoryMb, ScheduledFuture<?> finish) {}
}

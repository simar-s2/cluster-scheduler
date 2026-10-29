package clusterscheduler.experiments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Checks, from what the workers say they actually ran, whether any job ran
 * on two nodes at the same time. This is independent of the database: it
 * would catch a task that was started without being recorded, or a duplicate
 * delivery that a worker failed to ignore.
 */
final class WorkerLogCheck {
    private WorkerLogCheck() {}

    record Findings(
            long jobsStarted,
            long jobsRunOnTwoNodesAtOnce,
            long jobsStartedOnMoreThanOneNode,
            long assignmentsStartedTwice,
            long staleEpochRejections) {}

    /** One stretch of a job running on a node. A run that never ended counts as still going. */
    private record Run(String nodeId, long startMillis, long endMillis) {
        boolean overlaps(Run other) {
            return startMillis < other.endMillis && other.startMillis < endMillis;
        }
    }

    static Findings check(List<ExecutionEvent> eventsOldestFirst) {
        Map<String, String> jobByAssignment = new HashMap<>();
        Map<String, String> nodeByAssignment = new HashMap<>();
        Map<String, Long> startByAssignment = new HashMap<>();
        Map<String, Long> endByAssignment = new HashMap<>();
        long assignmentsStartedTwice = 0;
        long staleEpochRejections = 0;

        for (ExecutionEvent event : eventsOldestFirst) {
            switch (event.event()) {
                case "started" -> {
                    if (startByAssignment.containsKey(event.assignmentId())) {
                        assignmentsStartedTwice++;
                    }
                    startByAssignment.put(event.assignmentId(), event.timeMillis());
                    jobByAssignment.put(event.assignmentId(), event.jobId());
                    nodeByAssignment.put(event.assignmentId(), event.nodeId());
                }
                case "finished", "stopped" -> endByAssignment.put(event.assignmentId(), event.timeMillis());
                case "rejected_stale_epoch" -> staleEpochRejections++;
                default -> throw new IllegalArgumentException("Unknown event " + event.event());
            }
        }

        Map<String, List<Run>> runsByJob = new HashMap<>();
        for (String assignmentId : startByAssignment.keySet()) {
            Run run = new Run(
                    nodeByAssignment.get(assignmentId),
                    startByAssignment.get(assignmentId),
                    endByAssignment.getOrDefault(assignmentId, Long.MAX_VALUE));
            runsByJob.computeIfAbsent(jobByAssignment.get(assignmentId), job -> new ArrayList<>()).add(run);
        }

        long jobsRunOnTwoNodesAtOnce = 0;
        long jobsStartedOnMoreThanOneNode = 0;
        for (List<Run> runs : runsByJob.values()) {
            if (runs.stream().map(Run::nodeId).distinct().count() > 1) {
                jobsStartedOnMoreThanOneNode++;
            }
            if (hasOverlapOnDifferentNodes(runs)) {
                jobsRunOnTwoNodesAtOnce++;
            }
        }
        return new Findings(runsByJob.size(), jobsRunOnTwoNodesAtOnce, jobsStartedOnMoreThanOneNode,
                assignmentsStartedTwice, staleEpochRejections);
    }

    private static boolean hasOverlapOnDifferentNodes(List<Run> runs) {
        for (int i = 0; i < runs.size(); i++) {
            for (int j = i + 1; j < runs.size(); j++) {
                Run first = runs.get(i);
                Run second = runs.get(j);
                if (!first.nodeId().equals(second.nodeId()) && first.overlaps(second)) {
                    return true;
                }
            }
        }
        return false;
    }
}

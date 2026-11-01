package clusterscheduler.experiments;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Rebuilds the results table in README.md from results/*.json. Every number
 * in the table comes from a file an experiment wrote; nothing is typed in.
 * The table sits between two marker comments, and only that part of the
 * README is replaced.
 */
public final class ReadmeResults {
    private static final Path README = Path.of("README.md");
    private static final String START_MARKER = "<!-- results:start -->";
    private static final String END_MARKER = "<!-- results:end -->";

    private ReadmeResults() {}

    public static void main(String[] args) throws IOException {
        JsonNode chaos = read("failover-chaos");
        JsonNode nodeFailure = read("node-failure");
        JsonNode packing = read("packing");

        List<String> rows = new ArrayList<>();
        rows.add("| Measurement | Result | From |");
        rows.add("|---|---|---|");

        JsonNode failovers = field(chaos, "failovers");
        JsonNode untilNewLeader = field(failovers, "millisUntilNewLeader");
        JsonNode doubles = field(chaos, "doublePlacements");
        JsonNode fencing = field(chaos, "fencing");
        JsonNode jobs = field(chaos, "jobs");
        rows.add(row("Leader failovers (SIGKILL / pause)",
                field(failovers, "total").asLong() + " (" + field(failovers, "byKill").asLong() + " / "
                        + field(failovers, "byPause").asLong() + ")", "failover-chaos.json"));
        rows.add(row("Time until a new leader holds the lease, median / p99 / max",
                millis(field(untilNewLeader, "median")) + " / " + millis(field(untilNewLeader, "p99")) + " / "
                        + millis(field(untilNewLeader, "max")), "failover-chaos.json"));
        rows.add(row("Jobs submitted during the failovers / finished",
                count(field(jobs, "accepted")) + " / " + count(field(jobs, "finished")), "failover-chaos.json"));
        rows.add(row("**Double placements:** jobs with overlapping active assignments (database)",
                "**" + field(doubles, "jobsWithOverlappingActiveAssignmentsInDatabase").asLong() + "**",
                "failover-chaos.json"));
        rows.add(row("**Double placements:** jobs run on two nodes at once (worker logs)",
                "**" + field(doubles, "jobsRunOnTwoNodesAtOnceInWorkerLogs").asLong() + "**", "failover-chaos.json"));
        rows.add(row("Jobs ever assigned to more than one node / assignments started twice",
                field(doubles, "jobsAssignedToMoreThanOneNodeInDatabase").asLong() + " / "
                        + field(doubles, "assignmentsStartedTwiceInWorkerLogs").asLong(), "failover-chaos.json"));
        rows.add(row("Stale-leader writes rejected by the epoch check / frozen sessions ended by PostgreSQL",
                field(fencing, "controlPlaneWritesRejected").asLong() + " / "
                        + field(fencing, "controlPlaneTransactionsEndedByIdleTimeout").asLong(), "failover-chaos.json"));
        rows.add(row("Assignments workers rejected for a stale epoch",
                Long.toString(field(fencing, "workerStaleEpochRejections").asLong()), "failover-chaos.json"));

        JsonNode recovery = field(nodeFailure, "millisUntilAllTasksRunningElsewhere");
        JsonNode detection = field(nodeFailure, "millisUntilDeclaredDead");
        rows.add(row("Worker killed: time until all its tasks run elsewhere, median / max ("
                        + field(recovery, "count").asLong() + " trials)",
                millis(field(recovery, "median")) + " / " + millis(field(recovery, "max")), "node-failure.json"));
        rows.add(row("Worker killed: time until it is declared dead, median",
                millis(field(detection, "median")), "node-failure.json"));

        JsonNode cpu = field(packing, "cpuUtilizationWhileJobsWereWaiting");
        JsonNode memory = field(packing, "memoryUtilizationWhileJobsWereWaiting");
        JsonNode config = field(packing, "config");
        rows.add(row("**CPU utilization** (allocated / capacity) while jobs were waiting, mean / min",
                "**" + percent(field(cpu, "mean")) + "** / " + percent(field(cpu, "min")), "packing.json"));
        rows.add(row("Memory utilization while jobs were waiting, mean",
                percent(field(memory, "mean")), "packing.json"));
        rows.add(row(count(field(config, "jobs")) + " jobs on " + count(field(config, "nodes"))
                        + " nodes: time until all finished / jobs per second",
                String.format(Locale.ROOT, "%.0f s / %.1f", field(packing, "secondsUntilAllJobsFinished").asDouble(),
                        field(packing, "jobsFinishedPerSecond").asDouble()), "packing.json"));
        rows.add(row("Mean scheduling pass on the leader (up to 1,000 jobs x 100 nodes)",
                String.format(Locale.ROOT, "%.1f ms", field(packing, "meanSchedulingPassMillis").asDouble()),
                "packing.json"));

        JsonNode workerMemory = field(packing, "workerContainerMemoryMib");
        long nodesPerContainer = field(config, "nodesPerContainer").asLong();
        rows.add(row("Memory per worker container (" + (nodesPerContainer == 1 ? "one node" : nodesPerContainer + " nodes")
                        + ", small heap), mean / max",
                String.format(Locale.ROOT, "%.0f MiB / %.0f MiB", field(workerMemory, "mean").asDouble(),
                        field(workerMemory, "max").asDouble()), "packing.json"));

        replaceTable(String.join("\n", rows));
        System.out.println("Updated the results table in " + README);
    }

    private static JsonNode read(String name) throws IOException {
        return ResultsFile.JSON.readTree(ResultsFile.DIRECTORY.resolve(name + ".json").toFile());
    }

    /** A missing field fails loudly instead of quietly printing 0 in the README. */
    private static JsonNode field(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null) {
            throw new IllegalStateException("Results file has no field " + name);
        }
        return value;
    }

    private static String row(String measurement, String result, String file) {
        return "| " + measurement + " | " + result + " | [" + file + "](results/" + file + ") |";
    }

    private static String count(JsonNode value) {
        return String.format(Locale.ROOT, "%,d", value.asLong());
    }

    private static String millis(JsonNode value) {
        return String.format(Locale.ROOT, "%,d ms", Math.round(value.asDouble()));
    }

    private static String percent(JsonNode fraction) {
        return String.format(Locale.ROOT, "%.1f%%", 100 * fraction.asDouble());
    }

    private static void replaceTable(String table) throws IOException {
        String readme = Files.readString(README, StandardCharsets.UTF_8);
        int start = readme.indexOf(START_MARKER);
        int end = readme.indexOf(END_MARKER);
        if (start < 0 || end < start) {
            throw new IllegalStateException("README.md needs " + START_MARKER + " and " + END_MARKER);
        }
        String updated = readme.substring(0, start + START_MARKER.length()) + "\n" + table + "\n"
                + readme.substring(end);
        Files.writeString(README, updated, StandardCharsets.UTF_8);
    }
}

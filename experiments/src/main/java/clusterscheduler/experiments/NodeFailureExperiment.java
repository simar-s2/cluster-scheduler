package clusterscheduler.experiments;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kills one worker container and measures how long it takes until every task
 * that was running on it is running on another node again.
 *
 * The cluster is half full of long-running jobs, so there is always room to
 * move the dead node's tasks. Each trial kills the busiest live node. The
 * killed container is then started again; it registers as a new, empty node,
 * which keeps the cluster at full size for the next trial.
 */
public final class NodeFailureExperiment {
    private static final Logger log = LoggerFactory.getLogger(NodeFailureExperiment.class);

    record Config(
            int workers,
            int trials,
            int jobs,
            long jobCpuMillis,
            long jobMemoryMb,
            long heartbeatIntervalMillis,
            long heartbeatTimeoutMillis,
            long reconcileIntervalMillis,
            String clockStartsWhen) {}

    record Trial(int trial, int tasksOnKilledNode, long millisUntilDeclaredDead, long millisUntilAllTasksRunningElsewhere) {}

    record Results(
            String experiment,
            String finishedAt,
            Config config,
            List<Trial> trials,
            Stats millisUntilDeclaredDead,
            Stats millisUntilAllTasksRunningElsewhere,
            long jobsWithOverlappingActiveAssignmentsInDatabase) {}

    public static void main(String[] args) throws Exception {
        // The timing values are the control plane's and worker's defaults.
        Config config = new Config(
                Settings.number("WORKERS", 10), Settings.number("TRIALS", 5), 40, 500, 512, 1_000, 5_000, 1_000,
                "when docker reports the container killed");
        DockerCompose docker = new DockerCompose();
        ExecutionLogs.clear();
        log.info("Starting a cluster of 3 replicas and {} workers", config.workers());
        docker.startFreshCluster(config.workers(), 1);

        try (ClusterDatabase database = new ClusterDatabase(); JobClient client = new JobClient(0)) {
            waitForLiveNodes(database, config.workers());
            for (int i = 0; i < config.jobs(); i++) {
                // An hour is far longer than the experiment, so no task finishes on its own.
                client.submit("long-job-" + i, config.jobCpuMillis(), config.jobMemoryMb(), 3_600_000);
            }
            waitForRunningAssignments(database, config.jobs());

            List<Trial> trials = new ArrayList<>();
            for (int trialNumber = 1; trialNumber <= config.trials(); trialNumber++) {
                trials.add(runTrial(trialNumber, docker, database));
                waitForLiveNodes(database, config.workers());
                waitForRunningAssignments(database, config.jobs());
            }

            Results results = new Results(
                    "node-failure",
                    Instant.now().toString(),
                    config,
                    trials,
                    Stats.of(trials.stream().map(Trial::millisUntilDeclaredDead).toList()),
                    Stats.of(trials.stream().map(Trial::millisUntilAllTasksRunningElsewhere).toList()),
                    database.jobsWithOverlappingActiveAssignments());
            log.info("Wrote {}", ResultsFile.write("node-failure", results));
        } finally {
            docker.stopCluster();
        }
    }

    private static Trial runTrial(int trialNumber, DockerCompose docker, ClusterDatabase database) {
        String victimNodeId = database.queryTexts("""
                SELECT n.node_id FROM nodes n
                WHERE n.status = 'ALIVE'
                ORDER BY (SELECT count(*) FROM assignments a WHERE a.node_id = n.node_id AND a.state = 'RUNNING') DESC,
                         n.node_id
                LIMIT 1
                """).get(0);
        String victimIp = database.queryTexts("SELECT address FROM nodes WHERE node_id = ?", victimNodeId)
                .get(0).split(":")[0];
        String[] victimJobIds = database.queryTexts(
                "SELECT job_id FROM assignments WHERE node_id = ? AND state = 'RUNNING'", victimNodeId)
                .toArray(String[]::new);
        String containerId = docker.workerContainerIds().stream()
                .filter(id -> docker.containerIpAddress(id).equals(victimIp))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No worker container has IP " + victimIp));

        docker.killContainer(containerId);
        long killedAt = System.nanoTime();
        Waits.until("the killed node is declared dead", Duration.ofSeconds(60), () -> database.queryNumber(
                "SELECT count(*) FROM nodes WHERE node_id = ? AND status = 'DEAD'", victimNodeId) == 1);
        long declaredDeadAt = System.nanoTime();
        Waits.until("every task of the killed node runs elsewhere", Duration.ofSeconds(60), () -> database.queryNumber(
                "SELECT count(*) FROM assignments WHERE job_id = ANY (?) AND state = 'RUNNING' AND node_id <> ?",
                victimJobIds, victimNodeId) == victimJobIds.length);
        long allRunningElsewhereAt = System.nanoTime();

        Trial trial = new Trial(trialNumber, victimJobIds.length,
                (declaredDeadAt - killedAt) / 1_000_000, (allRunningElsewhereAt - killedAt) / 1_000_000);
        log.info("Trial {}: killed a node with {} tasks; declared dead after {} ms, all tasks running elsewhere after {} ms",
                trialNumber, trial.tasksOnKilledNode(), trial.millisUntilDeclaredDead(),
                trial.millisUntilAllTasksRunningElsewhere());
        docker.startContainer(containerId);
        return trial;
    }

    private static void waitForLiveNodes(ClusterDatabase database, int nodes) {
        Waits.until(nodes + " nodes are alive", Duration.ofMinutes(2),
                () -> database.queryNumber("SELECT count(*) FROM nodes WHERE status = 'ALIVE'") == nodes);
    }

    private static void waitForRunningAssignments(ClusterDatabase database, int assignments) {
        Waits.until(assignments + " tasks are running", Duration.ofMinutes(2),
                () -> database.queryNumber("SELECT count(*) FROM assignments WHERE state = 'RUNNING'") == assignments);
    }
}

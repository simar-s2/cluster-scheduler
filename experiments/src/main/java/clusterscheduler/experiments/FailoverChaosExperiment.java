package clusterscheduler.experiments;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes the leader down over and over while jobs keep arriving, then checks
 * that no job was ever placed on two nodes.
 *
 * Faults alternate between two kinds:
 * - kill: SIGKILL the leader's container, then start it again once a standby
 *   has taken over. A killed process can never write again, so this tests
 *   failover itself.
 * - pause: freeze the leader's container, then unfreeze it once a standby has
 *   taken over. The old leader wakes up believing it might still lead. This
 *   is the case fencing tokens exist for.
 *
 * Double placements are counted three ways, from two independent sources:
 * the assignment history in PostgreSQL, and the execution logs the workers
 * wrote about what they actually ran.
 */
public final class FailoverChaosExperiment {
    private static final Logger log = LoggerFactory.getLogger(FailoverChaosExperiment.class);
    private static final List<String> REPLICAS = List.of("control-plane-1", "control-plane-2", "control-plane-3");

    record Config(int failovers, int workers, int nodeCpuMillis, int nodeMemoryMb, int jobsPerSecond) {}

    record Failovers(int total, int byKill, int byPause, Stats millisUntilNewLeader) {}

    record Jobs(long accepted, long notAccepted, long finished, long acceptedButNeverFinished) {}

    record DoublePlacements(
            long jobsWithOverlappingActiveAssignmentsInDatabase,
            long jobsAssignedToMoreThanOneNodeInDatabase,
            long jobsRunOnTwoNodesAtOnceInWorkerLogs,
            long assignmentsStartedTwiceInWorkerLogs) {}

    /**
     * How often each safety mechanism actually fired. A paused leader can be
     * stopped two ways: its write reaches PostgreSQL and is rejected for its
     * epoch, or it was frozen inside a transaction and PostgreSQL ended the
     * session (idle_in_transaction_session_timeout) before it woke up.
     */
    record Fencing(
            long highestEpoch,
            long controlPlaneWritesRejected,
            long controlPlaneTransactionsEndedByIdleTimeout,
            long workerStaleEpochRejections) {}

    record Results(
            String experiment,
            String finishedAt,
            Config config,
            Failovers failovers,
            Jobs jobs,
            DoublePlacements doublePlacements,
            Fencing fencing,
            long nodesDeclaredDead) {}

    public static void main(String[] args) throws Exception {
        Config config = new Config(
                Settings.number("FAILOVERS", 200), Settings.number("WORKERS", 10), 4_000, 8_192,
                Settings.number("JOBS_PER_SECOND", 20));
        DockerCompose docker = new DockerCompose();
        ExecutionLogs.clear();
        log.info("Starting a cluster of 3 replicas and {} workers", config.workers());
        docker.startFreshCluster(config.workers(), 1);

        try (ClusterDatabase database = new ClusterDatabase(); JobClient client = new JobClient(0)) {
            Waits.until("every worker is registered", Duration.ofMinutes(2),
                    () -> database.queryNumber("SELECT count(*) FROM nodes WHERE status = 'ALIVE'") == config.workers());
            JobStream jobStream = new JobStream(client, config.jobsPerSecond());
            jobStream.start();

            Random random = new Random(7);
            List<Long> millisUntilNewLeader = new ArrayList<>();
            int kills = 0;
            int pauses = 0;
            for (int failover = 1; failover <= config.failovers(); failover++) {
                ClusterDatabase.Lease leader = waitForLeader(database);
                boolean kill = failover % 2 == 1;
                if (kill) {
                    docker.kill(leader.holder());
                    kills++;
                } else {
                    docker.pause(leader.holder());
                    pauses++;
                }
                long faultInPlaceAt = System.nanoTime();
                ClusterDatabase.Lease newLeader = waitForNewerLeader(database, leader.epoch());
                long millis = (System.nanoTime() - faultInPlaceAt) / 1_000_000;
                millisUntilNewLeader.add(millis);
                if (kill) {
                    docker.start(leader.holder());
                } else {
                    docker.unpause(leader.holder());
                }
                log.info("Failover {}/{}: {} {} (epoch {}), {} took over (epoch {}) after {} ms",
                        failover, config.failovers(), kill ? "killed" : "paused", leader.holder(), leader.epoch(),
                        newLeader.holder(), newLeader.epoch(), millis);
                // Let the new leader schedule for a while before the next fault.
                Waits.sleep(500 + random.nextInt(1_000));
            }

            jobStream.stop();
            List<String> acceptedJobIds = jobStream.acceptedJobIds();
            log.info("Stopped submitting after {} accepted jobs; waiting for them to finish", acceptedJobIds.size());
            String[] acceptedIds = acceptedJobIds.toArray(String[]::new);
            String finishedQuery = "SELECT count(*) FROM jobs WHERE state = 'FINISHED' AND job_id = ANY (?)";
            Waits.until("every accepted job has finished", Duration.ofMinutes(10),
                    () -> database.queryNumber(finishedQuery, (Object) acceptedIds) == acceptedIds.length);
            // One more heartbeat round, so every worker's last events are on disk.
            Waits.sleep(2_000);

            WorkerLogCheck.Findings workerFindings = WorkerLogCheck.check(ExecutionLogs.readAll());
            String replicaLogs = docker.logs(REPLICAS.toArray(String[]::new));
            long finished = database.queryNumber(finishedQuery, (Object) acceptedIds);
            Results results = new Results(
                    "failover-chaos",
                    Instant.now().toString(),
                    config,
                    new Failovers(config.failovers(), kills, pauses, Stats.of(millisUntilNewLeader)),
                    new Jobs(acceptedIds.length, jobStream.notAccepted(), finished, acceptedIds.length - finished),
                    new DoublePlacements(
                            database.jobsWithOverlappingActiveAssignments(),
                            database.jobsAssignedToMoreThanOneNode(),
                            workerFindings.jobsRunOnTwoNodesAtOnce(),
                            workerFindings.assignmentsStartedTwice()),
                    new Fencing(
                            database.readLease().epoch(),
                            countLines(replicaLogs, "FENCED:"),
                            countLines(replicaLogs, "idle-in-transaction timeout"),
                            workerFindings.staleEpochRejections()),
                    database.queryNumber("SELECT count(*) FROM nodes WHERE status = 'DEAD'"));
            log.info("Wrote {}", ResultsFile.write("failover-chaos", results));
        } finally {
            docker.stopCluster();
        }
    }

    private static ClusterDatabase.Lease waitForLeader(ClusterDatabase database) {
        Waits.until("some replica holds the lease", Duration.ofSeconds(30),
                () -> database.readLease().unexpired());
        return database.readLease();
    }

    private static ClusterDatabase.Lease waitForNewerLeader(ClusterDatabase database, long oldEpoch) {
        Waits.until("a replica holds the lease with an epoch above " + oldEpoch, Duration.ofSeconds(30), () -> {
            ClusterDatabase.Lease lease = database.readLease();
            return lease.unexpired() && lease.epoch() > oldEpoch;
        });
        return database.readLease();
    }

    private static long countLines(String text, String marker) {
        return text.lines().filter(line -> line.contains(marker)).count();
    }
}

package clusterscheduler.experiments;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Submits 10,000 jobs of mixed sizes to 100 nodes at once and measures how
 * fully the scheduler packs them.
 *
 * Utilization = allocated CPU / total CPU capacity, where allocated CPU is
 * the sum of the CPU requests of all active (ASSIGNED or RUNNING)
 * assignments and capacity is the sum over live nodes. It is sampled once a
 * second, and only samples taken while at least one job was waiting in the
 * queue count: then the cluster had work it could have run, so any unused
 * CPU is space the scheduler could not fill.
 */
public final class PackingBenchmark {
    private static final Logger log = LoggerFactory.getLogger(PackingBenchmark.class);

    record Config(
            int workerContainers,
            int nodesPerContainer,
            int nodes,
            long nodeCpuMillis,
            long nodeMemoryMb,
            int jobs,
            String jobCpuMillis,
            String jobMemoryMb,
            String jobDuration,
            long seed,
            String utilizationDefinition) {}

    record Sample(
            double secondsSinceStart,
            long pendingJobs,
            long activeAssignments,
            long finishedJobs,
            long allocatedCpuMillis,
            long allocatedMemoryMb,
            long cpuCapacityMillis,
            long memoryCapacityMb,
            long workerReportedUsedCpuMillis) {}

    record Results(
            String experiment,
            String finishedAt,
            Config config,
            int samplesWhileJobsWereWaiting,
            Stats cpuUtilizationWhileJobsWereWaiting,
            Stats memoryUtilizationWhileJobsWereWaiting,
            Stats workerReportedCpuUtilizationWhileJobsWereWaiting,
            double secondsUntilAllJobsFinished,
            double jobsFinishedPerSecond,
            Stats secondsEachJobWaitedForPlacement,
            double meanSchedulingPassMillis,
            Stats workerContainerMemoryMib,
            long nodesDeclaredDead,
            long jobsWithOverlappingActiveAssignmentsInDatabase,
            List<Sample> samples) {}

    private static final String SAMPLE_QUERY = """
            SELECT
              (SELECT count(*) FROM jobs WHERE state = 'PENDING'),
              (SELECT count(*) FROM assignments WHERE state IN ('ASSIGNED', 'RUNNING')),
              (SELECT count(*) FROM jobs WHERE state = 'FINISHED'),
              (SELECT COALESCE(sum(j.cpu_millis), 0) FROM assignments a JOIN jobs j ON j.job_id = a.job_id
                 WHERE a.state IN ('ASSIGNED', 'RUNNING')),
              (SELECT COALESCE(sum(j.memory_mb), 0) FROM assignments a JOIN jobs j ON j.job_id = a.job_id
                 WHERE a.state IN ('ASSIGNED', 'RUNNING')),
              (SELECT COALESCE(sum(cpu_capacity_millis), 0) FROM nodes WHERE status = 'ALIVE'),
              (SELECT COALESCE(sum(memory_capacity_mb), 0) FROM nodes WHERE status = 'ALIVE'),
              (SELECT COALESCE(sum(cpu_capacity_millis - reported_free_cpu_millis), 0) FROM nodes WHERE status = 'ALIVE')
            """;

    public static void main(String[] args) throws Exception {
        int workerContainers = Settings.number("WORKER_CONTAINERS", 100);
        int nodesPerContainer = Settings.number("NODES_PER_CONTAINER", 1);
        Config config = new Config(
                workerContainers, nodesPerContainer, workerContainers * nodesPerContainer, 4_000, 8_192,
                Settings.number("JOBS", 10_000),
                "250 to 2000 in steps of 250, uniform",
                "256 to 4096 in steps of 256, uniform",
                "5 to 20 seconds, uniform",
                1,
                "allocated CPU / total CPU capacity of live nodes, sampled every second while at least one job was pending");
        DockerCompose docker = new DockerCompose();
        ExecutionLogs.clear();
        log.info("Starting a cluster of 3 replicas and {} nodes ({} containers x {})",
                config.nodes(), workerContainers, nodesPerContainer);
        docker.startFreshCluster(workerContainers, nodesPerContainer);

        try (ClusterDatabase database = new ClusterDatabase()) {
            Waits.until(config.nodes() + " nodes are alive", Duration.ofMinutes(3),
                    () -> database.queryNumber("SELECT count(*) FROM nodes WHERE status = 'ALIVE'") == config.nodes());

            long startedAt = System.nanoTime();
            submitAll(config);
            log.info("Submitted {} jobs in {} ms", config.jobs(), (System.nanoTime() - startedAt) / 1_000_000);

            List<Sample> samples = new ArrayList<>();
            List<Double> workerMemoryMib = List.of();
            while (true) {
                long[] row = database.queryNumbers(SAMPLE_QUERY);
                Sample sample = new Sample((System.nanoTime() - startedAt) / 1e9,
                        row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]);
                samples.add(sample);
                if (samples.size() % 10 == 0) {
                    log.info("{} s: {} pending, {} running, {} finished, CPU {}% allocated",
                            Math.round(sample.secondsSinceStart()), sample.pendingJobs(), sample.activeAssignments(),
                            sample.finishedJobs(), Math.round(100.0 * sample.allocatedCpuMillis() / sample.cpuCapacityMillis()));
                }
                if (samples.size() == 30) {
                    // Half a minute in, every node is busy: a fair moment to see what a worker costs.
                    workerMemoryMib = docker.memoryUsageMib(docker.workerContainerIds());
                }
                if (sample.finishedJobs() == config.jobs()) {
                    break;
                }
                if (sample.secondsSinceStart() > 1_800) {
                    throw new IllegalStateException("Not finished after 30 minutes");
                }
                Waits.sleep(1_000);
            }
            double secondsUntilAllFinished = (System.nanoTime() - startedAt) / 1e9;

            // Samples while jobs were waiting, from the first one where anything had been placed.
            List<Sample> whileWaiting = samples.stream()
                    .filter(sample -> sample.pendingJobs() > 0 && sample.activeAssignments() > 0)
                    .toList();
            Results results = new Results(
                    "packing",
                    Instant.now().toString(),
                    config,
                    whileWaiting.size(),
                    Stats.of(whileWaiting.stream()
                            .map(sample -> (double) sample.allocatedCpuMillis() / sample.cpuCapacityMillis()).toList()),
                    Stats.of(whileWaiting.stream()
                            .map(sample -> (double) sample.allocatedMemoryMb() / sample.memoryCapacityMb()).toList()),
                    Stats.of(whileWaiting.stream()
                            .map(sample -> (double) sample.workerReportedUsedCpuMillis() / sample.cpuCapacityMillis())
                            .toList()),
                    secondsUntilAllFinished,
                    config.jobs() / secondsUntilAllFinished,
                    Stats.of(database.queryDecimals("""
                            SELECT EXTRACT(EPOCH FROM min(a.assigned_at) - j.submitted_at)
                            FROM jobs j JOIN assignments a ON a.job_id = j.job_id
                            GROUP BY j.job_id, j.submitted_at
                            """)),
                    meanSchedulingPassMillis(database.readLease().holder()),
                    Stats.of(workerMemoryMib),
                    // Should be 0. A worker killed by the out-of-memory killer would show up here.
                    database.queryNumber("SELECT count(*) FROM nodes WHERE status = 'DEAD'"),
                    database.jobsWithOverlappingActiveAssignments(),
                    samples);
            log.info("Wrote {}", ResultsFile.write("packing", results));
        } finally {
            docker.stopCluster();
        }
    }

    /** The same seeded sequence every run, submitted from 8 threads to spread over the replicas. */
    private static void submitAll(Config config) throws Exception {
        Random random = new Random(config.seed());
        List<long[]> jobs = new ArrayList<>();
        for (int i = 0; i < config.jobs(); i++) {
            jobs.add(new long[] {
                (1 + random.nextInt(8)) * 250L,     // CPU millicores
                (1 + random.nextInt(16)) * 256L,    // memory MB
                5_000 + random.nextInt(15_001)      // duration millis
            });
        }
        int threads = 8;
        ExecutorService submitters = Executors.newFixedThreadPool(threads);
        List<Future<?>> done = new ArrayList<>();
        for (int thread = 0; thread < threads; thread++) {
            int firstIndex = thread;
            done.add(submitters.submit(() -> {
                try (JobClient client = new JobClient(firstIndex)) {
                    for (int i = firstIndex; i < jobs.size(); i += threads) {
                        long[] job = jobs.get(i);
                        if (!client.submit(String.format("job-%05d", i), job[0], job[1], job[2])) {
                            throw new IllegalStateException("Job " + i + " was not accepted");
                        }
                    }
                }
                return null;
            }));
        }
        for (Future<?> future : done) {
            future.get();
        }
        submitters.shutdown();
    }

    /** Reads the leader's scheduling pass timer from its /metrics endpoint. */
    private static double meanSchedulingPassMillis(String leaderReplica) throws Exception {
        int metricsPort = 8080 + Integer.parseInt(leaderReplica.substring(leaderReplica.lastIndexOf('-') + 1));
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + metricsPort + "/metrics")).build();
        String body;
        try (HttpClient client = HttpClient.newHttpClient()) {
            body = client.send(request, HttpResponse.BodyHandlers.ofString()).body();
        }
        double seconds = metricValue(body, "scheduler_scheduling_pass_seconds_sum");
        double passes = metricValue(body, "scheduler_scheduling_pass_seconds_count");
        return passes == 0 ? 0 : 1_000 * seconds / passes;
    }

    private static double metricValue(String prometheusText, String name) {
        return prometheusText.lines()
                .filter(line -> line.startsWith(name + " "))
                .mapToDouble(line -> Double.parseDouble(line.substring(name.length() + 1).trim()))
                .findFirst()
                .orElse(0);
    }
}

package clusterscheduler.experiments;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Submits a steady stream of small, short jobs from a background thread until
 * stopped. Sizes and durations come from a seeded random generator, so every
 * run submits the same sequence.
 */
final class JobStream {
    private final JobClient client;
    private final int jobsPerSecond;
    private final Random random = new Random(42);
    private final List<String> acceptedJobIds = new CopyOnWriteArrayList<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread thread = new Thread(this::submitUntilStopped, "job-stream");
    private volatile long notAccepted;

    JobStream(JobClient client, int jobsPerSecond) {
        this.client = client;
        this.jobsPerSecond = jobsPerSecond;
    }

    void start() {
        thread.start();
    }

    void stop() throws InterruptedException {
        running.set(false);
        thread.join();
    }

    List<String> acceptedJobIds() {
        return acceptedJobIds;
    }

    long notAccepted() {
        return notAccepted;
    }

    private void submitUntilStopped() {
        long intervalNanos = 1_000_000_000L / jobsPerSecond;
        long nextSubmitAt = System.nanoTime();
        while (running.get()) {
            String jobId = UUID.randomUUID().toString();
            long cpuMillis = 100 + random.nextInt(10) * 100;     // 100 to 1000 millicores
            long memoryMb = 128 + random.nextInt(8) * 128;       // 128 to 1024 MB
            long durationMillis = 1_000 + random.nextInt(4_000); // 1 to 5 seconds
            if (client.submit(jobId, cpuMillis, memoryMb, durationMillis)) {
                acceptedJobIds.add(jobId);
            } else {
                notAccepted++;
            }
            nextSubmitAt += intervalNanos;
            long waitMillis = (nextSubmitAt - System.nanoTime()) / 1_000_000;
            if (waitMillis > 0) {
                Waits.sleep(waitMillis);
            }
        }
    }
}

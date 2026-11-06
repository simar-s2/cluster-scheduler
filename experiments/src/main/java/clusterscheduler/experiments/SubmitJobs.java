package clusterscheduler.experiments;

import java.util.UUID;

/**
 * Submits a few jobs to a running cluster, for trying it out by hand. They run
 * for an hour, long enough to watch them survive a crash.
 */
public final class SubmitJobs {
    private static final long CPU_MILLIS = 500;
    private static final long MEMORY_MB = 512;
    private static final long DURATION_MILLIS = 3_600_000;

    private SubmitJobs() {}

    public static void main(String[] args) {
        int count = Settings.number("COUNT", 12);
        try (JobClient client = new JobClient(0)) {
            for (int i = 0; i < count; i++) {
                if (!client.submit(UUID.randomUUID().toString(), CPU_MILLIS, MEMORY_MB, DURATION_MILLIS)) {
                    throw new IllegalStateException("No replica accepted the job. Is the cluster running?");
                }
            }
        }
        System.out.println("submitted " + count + " jobs");
    }
}

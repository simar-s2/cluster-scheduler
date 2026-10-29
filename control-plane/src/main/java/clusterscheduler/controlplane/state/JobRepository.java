package clusterscheduler.controlplane.state;

import clusterscheduler.controlplane.database.Database;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/** Reads and writes the jobs table: the desired state of the cluster. */
public final class JobRepository {
    private final Database database;

    public JobRepository(Database database) {
        this.database = database;
    }

    /**
     * Adds a job to the pending queue. Submitting an ID that already exists
     * changes nothing, so a client can retry a submission safely.
     */
    public void submit(String jobId, long cpuMillis, long memoryMb, long durationMillis) {
        database.inTransaction(connection -> {
            String sql = """
                    INSERT INTO jobs (job_id, cpu_millis, memory_mb, duration_millis)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (job_id) DO NOTHING
                    """;
            try (PreparedStatement insert = connection.prepareStatement(sql)) {
                insert.setString(1, jobId);
                insert.setLong(2, cpuMillis);
                insert.setLong(3, memoryMb);
                insert.setLong(4, durationMillis);
                return insert.executeUpdate();
            }
        });
    }

    /** The oldest pending jobs, first come first served. */
    public List<PendingJob> findPending(int limit) {
        return database.inTransaction(connection -> {
            String sql = """
                    SELECT job_id, cpu_millis, memory_mb, duration_millis
                    FROM jobs
                    WHERE state = 'PENDING'
                    ORDER BY submitted_at, job_id
                    LIMIT ?
                    """;
            try (PreparedStatement query = connection.prepareStatement(sql)) {
                query.setInt(1, limit);
                try (ResultSet rows = query.executeQuery()) {
                    List<PendingJob> pendingJobs = new ArrayList<>();
                    while (rows.next()) {
                        pendingJobs.add(new PendingJob(
                                rows.getString("job_id"),
                                rows.getLong("cpu_millis"),
                                rows.getLong("memory_mb"),
                                rows.getLong("duration_millis")));
                    }
                    return pendingJobs;
                }
            }
        });
    }
}

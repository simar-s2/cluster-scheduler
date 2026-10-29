package clusterscheduler.controlplane.state;

import clusterscheduler.controlplane.database.Database;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Applies one heartbeat to the database: the node is alive, these tasks are
 * running, these tasks finished. Everything in a heartbeat is a fact reported
 * by the worker, so any replica may record it.
 */
public final class HeartbeatRepository {
    private final Database database;

    public HeartbeatRepository(Database database) {
        this.database = database;
    }

    /**
     * Records the heartbeat in one transaction. Returns false if the node is
     * unknown, in which case nothing is recorded and the worker must register
     * again.
     */
    public boolean record(NodeHeartbeat heartbeat) {
        return database.inTransaction(connection -> {
            if (!touchNode(connection, heartbeat)) {
                return false;
            }
            markRunning(connection, heartbeat.nodeId(), heartbeat.runningAssignmentIds());
            List<String> finishedJobIds =
                    markAssignmentsFinished(connection, heartbeat.nodeId(), heartbeat.finishedAssignmentIds());
            markJobsFinished(connection, finishedJobIds);
            return true;
        });
    }

    private static boolean touchNode(Connection connection, NodeHeartbeat heartbeat) throws SQLException {
        String sql = """
                UPDATE nodes
                SET last_heartbeat_at = now(),
                    reported_free_cpu_millis = ?,
                    reported_free_memory_mb = ?
                WHERE node_id = ?
                """;
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            update.setLong(1, heartbeat.freeCpuMillis());
            update.setLong(2, heartbeat.freeMemoryMb());
            update.setString(3, heartbeat.nodeId());
            return update.executeUpdate() == 1;
        }
    }

    /** The worker confirmed it is running these, so they no longer need resending. */
    private static void markRunning(Connection connection, String nodeId, List<String> assignmentIds)
            throws SQLException {
        String sql = """
                UPDATE assignments
                SET state = 'RUNNING', running_at = now()
                WHERE node_id = ? AND state = 'ASSIGNED' AND assignment_id = ANY (?)
                """;
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            update.setString(1, nodeId);
            update.setArray(2, textArray(connection, assignmentIds));
            update.executeUpdate();
        }
    }

    /**
     * Finishes the node's active assignments with these IDs and returns their
     * job IDs. Only active assignments on this node match, so a repeated report
     * changes nothing.
     */
    private static List<String> markAssignmentsFinished(Connection connection, String nodeId, List<String> assignmentIds)
            throws SQLException {
        String sql = """
                UPDATE assignments
                SET state = 'FINISHED', ended_at = now()
                WHERE node_id = ? AND state IN ('ASSIGNED', 'RUNNING') AND assignment_id = ANY (?)
                RETURNING job_id
                """;
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            update.setString(1, nodeId);
            update.setArray(2, textArray(connection, assignmentIds));
            try (ResultSet rows = update.executeQuery()) {
                List<String> jobIds = new ArrayList<>();
                while (rows.next()) {
                    jobIds.add(rows.getString("job_id"));
                }
                return jobIds;
            }
        }
    }

    private static void markJobsFinished(Connection connection, List<String> jobIds) throws SQLException {
        String sql = """
                UPDATE jobs
                SET state = 'FINISHED', finished_at = now()
                WHERE state = 'SCHEDULED' AND job_id = ANY (?)
                """;
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            update.setArray(1, textArray(connection, jobIds));
            update.executeUpdate();
        }
    }

    private static Array textArray(Connection connection, List<String> values) throws SQLException {
        return connection.createArrayOf("text", values.toArray());
    }
}

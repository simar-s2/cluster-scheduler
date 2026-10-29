package clusterscheduler.controlplane.state;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes the assignments table. Methods take a Connection instead of opening
 * their own transaction, so the caller decides which checks run in the same
 * transaction.
 */
public final class AssignmentRepository {

    /**
     * Saves new assignments and marks their jobs SCHEDULED. Returns the ones
     * actually saved: a job that is no longer PENDING is skipped, so the same
     * job can never be claimed twice.
     */
    public List<Assignment> saveNew(Connection connection, List<Assignment> assignments) throws SQLException {
        List<Assignment> claimed = claimPendingJobs(connection, assignments);
        String sql = "INSERT INTO assignments (assignment_id, job_id, node_id) VALUES (?, ?, ?)";
        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            for (Assignment assignment : claimed) {
                insert.setString(1, assignment.assignmentId());
                insert.setString(2, assignment.jobId());
                insert.setString(3, assignment.nodeId());
                insert.addBatch();
            }
            insert.executeBatch();
        }
        return claimed;
    }

    /** Moves each job from PENDING to SCHEDULED, and keeps the assignments whose job moved. */
    private static List<Assignment> claimPendingJobs(Connection connection, List<Assignment> assignments)
            throws SQLException {
        String sql = "UPDATE jobs SET state = 'SCHEDULED' WHERE job_id = ? AND state = 'PENDING'";
        try (PreparedStatement update = connection.prepareStatement(sql)) {
            for (Assignment assignment : assignments) {
                update.setString(1, assignment.jobId());
                update.addBatch();
            }
            int[] rowsChanged = update.executeBatch();
            List<Assignment> claimed = new ArrayList<>();
            for (int i = 0; i < assignments.size(); i++) {
                if (rowsChanged[i] == 1) {
                    claimed.add(assignments.get(i));
                }
            }
            return claimed;
        }
    }
}

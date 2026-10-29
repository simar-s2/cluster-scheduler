package clusterscheduler.experiments;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads the cluster's state straight from PostgreSQL, the source of truth.
 * The experiments only read; they never change what the control plane wrote.
 */
final class ClusterDatabase implements AutoCloseable {
    private final Connection connection;

    ClusterDatabase() {
        try {
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://127.0.0.1:5432/scheduler", "scheduler", "scheduler");
        } catch (SQLException failure) {
            throw new IllegalStateException("Cannot connect to PostgreSQL", failure);
        }
    }

    record Lease(String holder, long epoch, boolean unexpired) {}

    Lease readLease() {
        String sql = "SELECT holder, epoch, expires_at > now() AS unexpired FROM leader_lease WHERE lease_id = 1";
        try (PreparedStatement query = connection.prepareStatement(sql);
                ResultSet row = query.executeQuery()) {
            row.next();
            return new Lease(row.getString("holder"), row.getLong("epoch"), row.getBoolean("unexpired"));
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    long queryNumber(String sql, Object... parameters) {
        try (PreparedStatement query = prepare(sql, parameters);
                ResultSet row = query.executeQuery()) {
            row.next();
            return row.getLong(1);
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    List<String> queryTexts(String sql, Object... parameters) {
        try (PreparedStatement query = prepare(sql, parameters);
                ResultSet rows = query.executeQuery()) {
            List<String> values = new ArrayList<>();
            while (rows.next()) {
                values.add(rows.getString(1));
            }
            return values;
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    /** One decimal number per row, for example a duration in seconds per job. */
    List<Double> queryDecimals(String sql, Object... parameters) {
        try (PreparedStatement query = prepare(sql, parameters);
                ResultSet rows = query.executeQuery()) {
            List<Double> values = new ArrayList<>();
            while (rows.next()) {
                values.add(rows.getDouble(1));
            }
            return values;
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    /** Several numbers from one row, for samples that must be taken at the same moment. */
    long[] queryNumbers(String sql, Object... parameters) {
        try (PreparedStatement query = prepare(sql, parameters);
                ResultSet row = query.executeQuery()) {
            row.next();
            int columns = row.getMetaData().getColumnCount();
            long[] values = new long[columns];
            for (int i = 0; i < columns; i++) {
                values[i] = row.getLong(i + 1);
            }
            return values;
        } catch (SQLException failure) {
            throw new IllegalStateException(failure);
        }
    }

    /**
     * Jobs that, according to the assignment history, had two assignments
     * active at overlapping times. An assignment is active from assigned_at
     * until ended_at (or now, if it has not ended).
     */
    long jobsWithOverlappingActiveAssignments() {
        return queryNumber("""
                SELECT count(DISTINCT first.job_id)
                FROM assignments first
                JOIN assignments second
                  ON second.job_id = first.job_id
                 AND second.assignment_id <> first.assignment_id
                 AND first.assigned_at < COALESCE(second.ended_at, 'infinity')
                 AND second.assigned_at < COALESCE(first.ended_at, 'infinity')
                """);
    }

    /** Jobs that were ever assigned to more than one node, overlapping or not. */
    long jobsAssignedToMoreThanOneNode() {
        return queryNumber("""
                SELECT count(*) FROM (
                    SELECT job_id FROM assignments GROUP BY job_id HAVING count(DISTINCT node_id) > 1
                ) AS moved
                """);
    }

    private PreparedStatement prepare(String sql, Object... parameters) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);
        }
        return statement;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }
}

package clusterscheduler.controlplane.state;

import clusterscheduler.controlplane.database.Database;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Reads and writes the nodes table. */
public final class NodeRepository {
    private final Database database;

    public NodeRepository(Database database) {
        this.database = database;
    }

    /** Adds a node and returns its new ID. Every registration is a new node. */
    public String register(String address, long cpuCapacityMillis, long memoryCapacityMb) {
        String nodeId = UUID.randomUUID().toString();
        database.inTransaction(connection -> {
            String sql = """
                    INSERT INTO nodes (node_id, address, cpu_capacity_millis, memory_capacity_mb,
                                       reported_free_cpu_millis, reported_free_memory_mb)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """;
            try (PreparedStatement insert = connection.prepareStatement(sql)) {
                insert.setString(1, nodeId);
                insert.setString(2, address);
                insert.setLong(3, cpuCapacityMillis);
                insert.setLong(4, memoryCapacityMb);
                insert.setLong(5, cpuCapacityMillis);
                insert.setLong(6, memoryCapacityMb);
                insert.executeUpdate();
            }
            return null;
        });
        return nodeId;
    }

    /**
     * Every node with its free space, sorted by node ID. Free space comes from
     * the assignments table, not from heartbeats, because heartbeats lag
     * behind: a task placed a moment ago is not in any heartbeat yet.
     */
    public List<NodeSpace> findNodesWithFreeSpace() {
        return database.inTransaction(connection -> {
            String sql = """
                    SELECT n.node_id, n.address, n.cpu_capacity_millis, n.memory_capacity_mb,
                           n.cpu_capacity_millis - COALESCE(SUM(j.cpu_millis), 0) AS free_cpu_millis,
                           n.memory_capacity_mb - COALESCE(SUM(j.memory_mb), 0) AS free_memory_mb
                    FROM nodes n
                    LEFT JOIN assignments a
                           ON a.node_id = n.node_id AND a.state IN ('ASSIGNED', 'RUNNING')
                    LEFT JOIN jobs j ON j.job_id = a.job_id
                    GROUP BY n.node_id
                    ORDER BY n.node_id
                    """;
            try (PreparedStatement query = connection.prepareStatement(sql);
                    ResultSet rows = query.executeQuery()) {
                List<NodeSpace> nodes = new ArrayList<>();
                while (rows.next()) {
                    nodes.add(new NodeSpace(
                            rows.getString("node_id"),
                            rows.getString("address"),
                            rows.getLong("cpu_capacity_millis"),
                            rows.getLong("memory_capacity_mb"),
                            rows.getLong("free_cpu_millis"),
                            rows.getLong("free_memory_mb")));
                }
                return nodes;
            }
        });
    }

}

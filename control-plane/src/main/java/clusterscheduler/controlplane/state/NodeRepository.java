package clusterscheduler.controlplane.state;

import clusterscheduler.controlplane.database.Database;
import java.sql.PreparedStatement;
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
}

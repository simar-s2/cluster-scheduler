package clusterscheduler.controlplane;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.controlplane.database.SchemaInitializer;
import clusterscheduler.controlplane.state.NodeRepository;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/** One control plane replica: builds every component and owns their lifecycle. */
public final class ControlPlane implements AutoCloseable {
    private final Database database;
    private final Server grpcServer;

    private ControlPlane(Database database, Server grpcServer) {
        this.database = database;
        this.grpcServer = grpcServer;
    }

    public static ControlPlane start(ControlPlaneConfig config) throws IOException {
        Database database = new Database(config.databaseUrl(), config.databaseUser(), config.databasePassword());
        SchemaInitializer.apply(database);
        NodeRepository nodeRepository = new NodeRepository(database);

        Server grpcServer = ServerBuilder.forPort(config.grpcPort())
                .addService(new ControlPlaneService(nodeRepository))
                .build()
                .start();
        return new ControlPlane(database, grpcServer);
    }

    /** The port the gRPC server listens on. Useful when the config asked for port 0. */
    public int grpcPort() {
        return grpcServer.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        grpcServer.awaitTermination();
    }

    @Override
    public void close() {
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(2, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            grpcServer.shutdownNow();
            Thread.currentThread().interrupt();
        }
        database.close();
    }
}

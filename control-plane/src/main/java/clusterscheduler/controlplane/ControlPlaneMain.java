package clusterscheduler.controlplane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Entry point for one control plane replica. */
public final class ControlPlaneMain {
    private static final Logger log = LoggerFactory.getLogger(ControlPlaneMain.class);

    private ControlPlaneMain() {}

    public static void main(String[] args) throws Exception {
        ControlPlaneConfig config = ControlPlaneConfig.fromVariables(System.getenv());
        ControlPlane controlPlane = ControlPlane.start(config);
        log.info("Control plane replica {} listening on port {}", config.replicaId(), controlPlane.grpcPort());
        Runtime.getRuntime().addShutdownHook(new Thread(controlPlane::close));
        controlPlane.awaitTermination();
    }
}

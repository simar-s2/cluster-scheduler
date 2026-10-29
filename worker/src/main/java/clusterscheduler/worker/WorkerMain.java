package clusterscheduler.worker;

import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Entry point for one worker node. */
public final class WorkerMain {
    private WorkerMain() {}

    public static void main(String[] args) throws Exception {
        // The JVM caches DNS answers for 30 seconds by default. A control plane
        // container that restarts can come back with a new IP, and the worker
        // should find it again on its next reconnect, not half a minute later.
        Security.setProperty("networkaddress.cache.ttl", "1");
        WorkerConfig config = WorkerConfig.fromVariables(System.getenv());
        List<WorkerNode> nodes = new ArrayList<>();
        for (int nodeNumber = 0; nodeNumber < config.nodesPerProcess(); nodeNumber++) {
            nodes.add(WorkerNode.start(config.forNodeNumber(nodeNumber)));
        }
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            nodes.forEach(WorkerNode::close);
            stopped.countDown();
        }));
        stopped.await();
    }
}

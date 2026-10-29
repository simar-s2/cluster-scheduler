package clusterscheduler.worker;

import java.security.Security;
import java.util.concurrent.CountDownLatch;

/** Entry point for one worker node. */
public final class WorkerMain {
    private WorkerMain() {}

    public static void main(String[] args) throws Exception {
        // The JVM caches DNS answers for 30 seconds by default. A control plane
        // container that restarts can come back with a new IP, and the worker
        // should find it again on its next reconnect, not half a minute later.
        Security.setProperty("networkaddress.cache.ttl", "1");
        WorkerNode workerNode = WorkerNode.start(WorkerConfig.fromVariables(System.getenv()));
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            workerNode.close();
            stopped.countDown();
        }));
        stopped.await();
    }
}

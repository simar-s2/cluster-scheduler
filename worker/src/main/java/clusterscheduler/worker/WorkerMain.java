package clusterscheduler.worker;

import java.util.concurrent.CountDownLatch;

/** Entry point for one worker node. */
public final class WorkerMain {
    private WorkerMain() {}

    public static void main(String[] args) throws InterruptedException {
        WorkerNode workerNode = WorkerNode.start(WorkerConfig.fromEnvironment());
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            workerNode.close();
            stopped.countDown();
        }));
        stopped.await();
    }
}

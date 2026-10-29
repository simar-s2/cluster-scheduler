package clusterscheduler.controlplane;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/** Waits for something that happens asynchronously, such as a heartbeat landing. */
final class Eventually {
    private Eventually() {}

    static void waitUntil(String description, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out after " + timeout + " waiting until " + description);
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted while waiting until " + description);
            }
        }
    }
}

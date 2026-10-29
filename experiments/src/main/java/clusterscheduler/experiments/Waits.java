package clusterscheduler.experiments;

import java.time.Duration;
import java.util.function.BooleanSupplier;

/** Polling helpers for conditions that become true asynchronously. */
final class Waits {
    private Waits() {}

    static void until(String description, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Timed out after " + timeout + " waiting until " + description);
            }
            sleep(50);
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted");
        }
    }
}

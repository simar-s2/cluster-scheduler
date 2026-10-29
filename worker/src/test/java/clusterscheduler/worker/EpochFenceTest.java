package clusterscheduler.worker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class EpochFenceTest {
    private final EpochFence fence = new EpochFence();

    @Test
    void acceptsTheSameEpochAgainAndNewerOnes() {
        assertTrue(fence.admit(3));
        assertTrue(fence.admit(3));
        assertTrue(fence.admit(4));
    }

    @Test
    void rejectsAnOlderEpochOnceANewerOneWasSeen() {
        fence.admit(5);

        assertFalse(fence.admit(4));
        assertTrue(fence.admit(5));
    }
}

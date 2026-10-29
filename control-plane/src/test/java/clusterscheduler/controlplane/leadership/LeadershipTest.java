package clusterscheduler.controlplane.leadership;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class LeadershipTest {
    private long fakeNanos = 1_000;
    private final Leadership leadership = new Leadership(() -> fakeNanos);

    @Test
    void aNewReplicaIsNotLeader() {
        assertFalse(leadership.isLeader());
    }

    @Test
    void leadsUntilTheDeadlineAndNotAfter() {
        leadership.becomeLeader(7, 2_000);
        assertEquals(OptionalLong.of(7), leadership.currentEpoch());

        fakeNanos = 2_000;
        assertEquals(OptionalLong.empty(), leadership.currentEpoch());
        assertEquals(7, leadership.heldEpoch());
    }

    @Test
    void renewalMovesTheDeadline() {
        leadership.becomeLeader(7, 2_000);
        leadership.extend(7, 5_000);

        fakeNanos = 4_000;
        assertEquals(OptionalLong.of(7), leadership.currentEpoch());
    }

    @Test
    void stepDownFromAnOldTermDoesNotEndTheCurrentOne() {
        leadership.becomeLeader(7, 2_000);
        leadership.stepDown(7);
        leadership.becomeLeader(8, 2_000);

        leadership.stepDown(7);

        assertEquals(OptionalLong.of(8), leadership.currentEpoch());
    }
}

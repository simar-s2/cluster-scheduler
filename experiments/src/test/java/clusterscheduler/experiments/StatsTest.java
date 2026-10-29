package clusterscheduler.experiments;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class StatsTest {

    @Test
    void summarizesOneToOneHundred() {
        Stats stats = Stats.of(IntStream.rangeClosed(1, 100).boxed().toList());

        assertEquals(100, stats.count());
        assertEquals(1, stats.min());
        assertEquals(50, stats.median());
        assertEquals(99, stats.p99());
        assertEquals(100, stats.max());
        assertEquals(50.5, stats.mean());
    }

    @Test
    void handlesASingleValueAndNoValues() {
        assertEquals(7, Stats.of(List.of(7)).p99());
        assertEquals(0, Stats.of(List.of()).count());
    }
}

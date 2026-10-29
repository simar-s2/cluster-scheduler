package clusterscheduler.controlplane.scheduling;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import clusterscheduler.controlplane.state.NodeSpace;
import clusterscheduler.controlplane.state.PendingJob;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BestFitSchedulerTest {
    private final BestFitScheduler scheduler = new BestFitScheduler();

    @Test
    void picksTheNodeWithTheLeastSpaceLeftOver() {
        NodeSpace roomy = node("a-roomy", 4_000, 8_000, 4_000, 8_000);
        NodeSpace snug = node("b-snug", 4_000, 8_000, 1_000, 2_000);

        List<Placement> placements = scheduler.place(List.of(job("job", 1_000, 2_000)), List.of(roomy, snug));

        assertEquals(1, placements.size());
        assertEquals("b-snug", placements.get(0).node().nodeId());
    }

    @Test
    void requiresBothCpuAndMemoryToFit() {
        NodeSpace enoughCpuOnly = node("a", 4_000, 8_000, 4_000, 100);
        NodeSpace enoughMemoryOnly = node("b", 4_000, 8_000, 100, 8_000);

        List<Placement> placements =
                scheduler.place(List.of(job("job", 1_000, 1_000)), List.of(enoughCpuOnly, enoughMemoryOnly));

        assertTrue(placements.isEmpty());
    }

    @Test
    void accountsForJobsPlacedEarlierInTheSamePass() {
        NodeSpace onlyNode = node("a", 2_000, 4_000, 2_000, 4_000);

        List<Placement> placements = scheduler.place(
                List.of(job("first", 1_500, 1_000), job("second", 1_000, 1_000)), List.of(onlyNode));

        assertEquals(List.of("first"), placements.stream().map(p -> p.job().jobId()).toList());
    }

    @Test
    void aJobThatFitsNowhereDoesNotBlockSmallerJobsBehindIt() {
        NodeSpace onlyNode = node("a", 2_000, 4_000, 2_000, 4_000);

        List<Placement> placements = scheduler.place(
                List.of(job("too-big", 3_000, 1_000), job("small", 500, 500)), List.of(onlyNode));

        assertEquals(List.of("small"), placements.stream().map(p -> p.job().jobId()).toList());
    }

    @Test
    void breaksTiesByListOrderSoDecisionsAreRepeatable() {
        NodeSpace first = node("a", 4_000, 8_000, 4_000, 8_000);
        NodeSpace second = node("b", 4_000, 8_000, 4_000, 8_000);

        List<Placement> placements = scheduler.place(List.of(job("job", 1_000, 1_000)), List.of(first, second));

        assertEquals("a", placements.get(0).node().nodeId());
    }

    @Test
    void neverPlacesMoreThanANodeCanHold() {
        List<NodeSpace> nodes = List.of(
                node("a", 4_000, 8_000, 4_000, 8_000),
                node("b", 4_000, 8_000, 4_000, 8_000),
                node("c", 4_000, 8_000, 4_000, 8_000));
        List<PendingJob> jobs = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            jobs.add(job("job-" + i, 300 + (i * 37) % 900, 500 + (i * 53) % 1_500));
        }

        Map<String, long[]> usedByNode = new HashMap<>();
        for (Placement placement : scheduler.place(jobs, nodes)) {
            long[] used = usedByNode.computeIfAbsent(placement.node().nodeId(), id -> new long[2]);
            used[0] += placement.job().cpuMillis();
            used[1] += placement.job().memoryMb();
        }

        usedByNode.values().forEach(used -> {
            assertTrue(used[0] <= 4_000, "CPU overcommitted: " + used[0]);
            assertTrue(used[1] <= 8_000, "memory overcommitted: " + used[1]);
        });
    }

    private static NodeSpace node(String id, long cpuCapacity, long memoryCapacity, long freeCpu, long freeMemory) {
        return new NodeSpace(id, id + ":9100", cpuCapacity, memoryCapacity, freeCpu, freeMemory);
    }

    private static PendingJob job(String id, long cpuMillis, long memoryMb) {
        return new PendingJob(id, cpuMillis, memoryMb, 1_000);
    }
}

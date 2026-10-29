package clusterscheduler.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ExecutionLogTest {

    @Test
    void writesOneJsonObjectPerEventAndEscapesQuotes(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("worker.jsonl");
        try (ExecutionLog log = ExecutionLog.appendingTo(file)) {
            log.started("node-1", "a1", "job \"quoted\"", 3);
            log.finished("node-1", "a1", "job \"quoted\"");
        }

        List<String> lines = Files.readAllLines(file);
        assertEquals(2, lines.size());
        assertTrue(lines.get(0).startsWith(
                "{\"event\":\"started\",\"nodeId\":\"node-1\",\"assignmentId\":\"a1\",\"jobId\":\"job \\\"quoted\\\"\",\"epoch\":3,"));
        assertTrue(lines.get(1).startsWith("{\"event\":\"finished\""));
    }

    @Test
    void appendsToAnExistingFileAfterARestart(@TempDir Path directory) throws Exception {
        Path file = directory.resolve("worker.jsonl");
        try (ExecutionLog log = ExecutionLog.appendingTo(file)) {
            log.started("node-1", "a1", "job-1", 1);
        }
        try (ExecutionLog log = ExecutionLog.appendingTo(file)) {
            log.started("node-2", "a2", "job-2", 1);
        }

        assertEquals(2, Files.readAllLines(file).size());
    }
}

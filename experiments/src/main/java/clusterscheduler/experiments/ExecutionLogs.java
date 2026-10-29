package clusterscheduler.experiments;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/** Reads every worker's execution log from build/worker-logs, which compose mounts into each worker. */
final class ExecutionLogs {
    static final Path DIRECTORY = Path.of("build", "worker-logs");
    private static final ObjectMapper JSON = new ObjectMapper();

    private ExecutionLogs() {}

    /** Deletes old logs, so a run only sees its own events. */
    static void clear() {
        if (!Files.exists(DIRECTORY)) {
            return;
        }
        try (Stream<Path> files = Files.list(DIRECTORY)) {
            for (Path file : files.toList()) {
                Files.delete(file);
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    /** Every event from every worker, oldest first. */
    static List<ExecutionEvent> readAll() {
        List<ExecutionEvent> events = new ArrayList<>();
        try (Stream<Path> files = Files.list(DIRECTORY)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".jsonl")).toList()) {
                for (String line : Files.readAllLines(file)) {
                    if (!line.isBlank()) {
                        events.add(JSON.readValue(line, ExecutionEvent.class));
                    }
                }
            }
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
        events.sort(Comparator.comparingLong(ExecutionEvent::timeMillis));
        return events;
    }
}

package clusterscheduler.worker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An append-only record of what this worker actually ran, one JSON object
 * per line. The experiments read these files afterwards to check, from the
 * workers' side, that no job ever ran on two nodes at once.
 *
 * Each line is flushed as soon as it is written, so a worker killed with
 * SIGKILL still leaves every line it wrote in the file.
 */
final class ExecutionLog implements AutoCloseable {
    private final BufferedWriter writer;

    private ExecutionLog(BufferedWriter writer) {
        this.writer = writer;
    }

    /** A log that writes nothing, for when no log directory is configured. */
    static ExecutionLog disabled() {
        return new ExecutionLog(null);
    }

    static ExecutionLog appendingTo(Path file) {
        try {
            Files.createDirectories(file.getParent());
            return new ExecutionLog(Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    void started(String nodeId, String assignmentId, String jobId, long epoch) {
        write("started", nodeId, assignmentId, jobId, epoch);
    }

    void finished(String nodeId, String assignmentId, String jobId) {
        write("finished", nodeId, assignmentId, jobId, 0);
    }

    /** The task was stopped early because the control plane declared this node dead. */
    void stopped(String nodeId, String assignmentId, String jobId) {
        write("stopped", nodeId, assignmentId, jobId, 0);
    }

    void rejectedStaleEpoch(String nodeId, String assignmentId, String jobId, long epoch) {
        write("rejected_stale_epoch", nodeId, assignmentId, jobId, epoch);
    }

    private synchronized void write(String event, String nodeId, String assignmentId, String jobId, long epoch) {
        if (writer == null) {
            return;
        }
        String line = "{\"event\":" + quote(event)
                + ",\"nodeId\":" + quote(nodeId)
                + ",\"assignmentId\":" + quote(assignmentId)
                + ",\"jobId\":" + quote(jobId)
                + ",\"epoch\":" + epoch
                + ",\"timeMillis\":" + System.currentTimeMillis() + "}";
        try {
            writer.write(line);
            writer.newLine();
            writer.flush();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    /** JSON string quoting. Job IDs come from clients, so quotes and backslashes are escaped. */
    private static String quote(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

    @Override
    public synchronized void close() {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }
}

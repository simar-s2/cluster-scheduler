package clusterscheduler.experiments;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/** Runs docker and docker compose commands against this repository's docker-compose.yml. */
final class DockerCompose {

    /** Starts a fresh cluster: three replicas, PostgreSQL, and the requested workers. */
    void startFreshCluster(int workerContainers, int nodesPerContainer) {
        stopCluster();
        runWithEnvironment(Map.of("NODES_PER_PROCESS", Integer.toString(nodesPerContainer)),
                "docker", "compose", "up", "--detach", "--build", "--wait", "--scale", "worker=" + workerContainers);
    }

    /** Stops every container and deletes the database volume. */
    void stopCluster() {
        run("docker", "compose", "down", "--volumes", "--remove-orphans");
    }

    /** SIGKILL, so the process gets no chance to clean up. */
    void kill(String service) {
        run("docker", "compose", "kill", "--signal", "SIGKILL", service);
    }

    void start(String service) {
        run("docker", "compose", "start", service);
    }

    /** Freezes every process in the container, like a very long GC pause. */
    void pause(String service) {
        run("docker", "compose", "pause", service);
    }

    void unpause(String service) {
        run("docker", "compose", "unpause", service);
    }

    List<String> workerContainerIds() {
        String output = run("docker", "compose", "ps", "--quiet", "worker").trim();
        return output.isEmpty() ? List.of() : Arrays.asList(output.split("\\s+"));
    }

    String containerIpAddress(String containerId) {
        return run("docker", "inspect", "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", containerId).trim();
    }

    void killContainer(String containerId) {
        run("docker", "kill", "--signal", "SIGKILL", containerId);
    }

    void startContainer(String containerId) {
        run("docker", "start", containerId);
    }

    /** Everything the given services have logged, across restarts. */
    String logs(String... services) {
        List<String> command = new ArrayList<>(List.of("docker", "compose", "logs", "--no-color"));
        command.addAll(List.of(services));
        return run(command.toArray(String[]::new));
    }

    private static String run(String... command) {
        return runWithEnvironment(Map.of(), command);
    }

    private static String runWithEnvironment(Map<String, String> environment, String... command) {
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().putAll(environment);
        try {
            Process process = builder.start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IllegalStateException(String.join(" ", command) + " failed (" + exitCode + "):\n" + output);
            }
            return output;
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while running " + String.join(" ", command));
        }
    }
}

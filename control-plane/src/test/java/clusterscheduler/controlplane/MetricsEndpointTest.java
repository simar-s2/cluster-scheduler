package clusterscheduler.controlplane;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetricsEndpointTest {
    private TestCluster cluster;

    @BeforeEach
    void startCluster() {
        cluster = TestCluster.start(1, 1, Map.of());
        cluster.waitUntilAllWorkersRegistered();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    void reportsLeadershipSubmissionsAndPlacementsInPrometheusFormat() throws Exception {
        cluster.submitJob("a", 500, 500, 60_000);
        cluster.submitJob("b", 500, 500, 60_000);
        Eventually.waitUntil("both jobs are placed", Duration.ofSeconds(10),
                () -> cluster.queryNumber("SELECT count(*) FROM assignments") == 2);

        String body = scrape(cluster.controlPlane(0).metricsPort());

        assertTrue(body.contains("scheduler_is_leader 1.0"), body);
        assertTrue(body.contains("scheduler_jobs_submitted_total 2.0"), body);
        assertTrue(body.contains("scheduler_placements_saved_total 2.0"), body);
        assertTrue(body.contains("scheduler_heartbeats_received_total"), body);
    }

    private static String scrape(int port) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).build();
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(request, HttpResponse.BodyHandlers.ofString()).body();
        }
    }
}

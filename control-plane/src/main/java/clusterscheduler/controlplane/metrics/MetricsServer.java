package clusterscheduler.controlplane.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/** Serves GET /metrics for Prometheus to scrape, using the HTTP server built into the JDK. */
public final class MetricsServer implements AutoCloseable {
    private final HttpServer server;

    public MetricsServer(Metrics metrics, String bindHost, int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);
        server.createContext("/metrics", exchange -> respond(exchange, metrics.scrape()));
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    private static void respond(HttpExchange exchange, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}

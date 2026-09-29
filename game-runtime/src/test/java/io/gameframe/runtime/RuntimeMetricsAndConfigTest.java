package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeMetricsAndConfigTest {
    @Test
    void metricsAreBoundedSortedAndExportable() {
        var metrics = new RuntimeMetrics(2);
        metrics.increment("rpc_calls");
        metrics.increment("rpc_calls", 2);
        metrics.gauge("connections", 7);
        assertEquals(Map.of("rpc_calls", 3L), metrics.snapshot().counters());
        assertEquals(Map.of("connections", 7L), metrics.snapshot().gauges());
        assertEquals("rpc_calls_total 3\nconnections 7\n", metrics.toPrometheus());
        assertThrows(IllegalStateException.class, () -> metrics.gauge("third_metric", 1));
        assertThrows(IllegalArgumentException.class, () -> metrics.increment("player.1"));
    }

    @Test
    void metricsSupportConcurrentUpdates() throws Exception {
        var metrics = new RuntimeMetrics();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = java.util.stream.IntStream.range(0, 100)
                    .mapToObj(ignored -> executor.submit(() -> metrics.increment("jobs")))
                    .toList();
            for (var task : tasks) task.get();
        }
        assertEquals(100L, metrics.snapshot().counters().get("jobs"));
    }

    @Test
    void configDefaultsAndPropertyParsingValidateInput() {
        var defaults = GameServerConfig.defaults("node-a", "game");
        assertEquals(20, defaults.tickRate());
        var config = GameServerConfig.from(Map.of("nodeId", "node-b", "serviceRole", "gate",
                "tcpPort", "7000", "shutdownTimeoutMillis", "5000"));
        assertEquals(7000, config.tcpPort());
        assertEquals(Duration.ofSeconds(5), config.shutdownTimeout());
        assertThrows(IllegalArgumentException.class, () -> GameServerConfig.from(Map.of("nodeId", "n", "serviceRole", "g", "tickRate", "0")));
        assertThrows(IllegalArgumentException.class, () -> new GameServerConfig("n", "g", "*", 1, 1, 1, 20, Duration.ZERO));
    }
}
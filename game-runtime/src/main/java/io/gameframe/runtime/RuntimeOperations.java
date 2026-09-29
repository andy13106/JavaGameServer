package io.gameframe.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Single read boundary for health, bounded metrics and graceful-drain state.
 * Hosts can expose this snapshot through their existing admin endpoint without
 * coupling game logic to a metrics or health vendor.
 */
public final class RuntimeOperations {
    public record Snapshot(long capturedAtMillis, HealthRegistry.Snapshot health,
                           RuntimeMetrics.Snapshot metrics, GracefulDrain.State drainState,
                           int inFlight) {
        public Snapshot {
            if (capturedAtMillis < 0) throw new IllegalArgumentException("capturedAtMillis must not be negative");
            Objects.requireNonNull(health, "health");
            Objects.requireNonNull(metrics, "metrics");
            Objects.requireNonNull(drainState, "drainState");
            if (inFlight < 0) throw new IllegalArgumentException("inFlight must not be negative");
        }
    }

    private final HealthRegistry health;
    private final RuntimeMetrics metrics;
    private final GracefulDrain drain;

    public RuntimeOperations(HealthRegistry health, RuntimeMetrics metrics, GracefulDrain drain) {
        this.health = Objects.requireNonNull(health, "health");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.drain = Objects.requireNonNull(drain, "drain");
    }

    public CompletionStage<Snapshot> snapshot(Duration healthTimeout, long nowMillis) {
        Objects.requireNonNull(healthTimeout, "healthTimeout");
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        return health.check(healthTimeout, nowMillis)
                .thenApply(status -> new Snapshot(nowMillis, status, metrics.snapshot(), drain.state(), drain.inFlight()));
    }

    /** Returns metrics plus bounded drain gauges in Prometheus text format. */
    public String prometheus() {
        StringBuilder output = new StringBuilder(metrics.toPrometheus());
        output.append("gameframe_drain_state ").append(drain.state().ordinal()).append('\n');
        output.append("gameframe_drain_in_flight ").append(drain.inFlight()).append('\n');
        return output.toString();
    }
}
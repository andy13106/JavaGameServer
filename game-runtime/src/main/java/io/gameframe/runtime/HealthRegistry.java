package io.gameframe.runtime;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Small asynchronous health-check registry with bounded check count and timeout isolation.
 */
public final class HealthRegistry {
    public enum State { UP, DEGRADED, DOWN, UNKNOWN }

    @FunctionalInterface
    public interface Check {
        CompletionStage<State> run();
    }

    public record ComponentStatus(String name, State state, String detail, long latencyMillis) {
        public ComponentStatus {
            requireText(name, "name");
            Objects.requireNonNull(state, "state");
            if (detail == null || detail.length() > 512) throw new IllegalArgumentException("invalid detail");
            if (latencyMillis < 0) throw new IllegalArgumentException("latencyMillis must not be negative");
        }
    }

    public record Snapshot(long checkedAtMillis, State overall, List<ComponentStatus> components) {
        public Snapshot {
            if (checkedAtMillis < 0) throw new IllegalArgumentException("checkedAtMillis must not be negative");
            Objects.requireNonNull(overall, "overall");
            components = List.copyOf(components);
        }
    }

    private final int maxChecks;
    private final Map<String, Check> checks = new ConcurrentHashMap<>();

    public HealthRegistry(int maxChecks) {
        if (maxChecks < 1) throw new IllegalArgumentException("maxChecks must be positive");
        this.maxChecks = maxChecks;
    }

    public void register(String name, Check check) {
        requireText(name, "name");
        Objects.requireNonNull(check, "check");
        if (checks.putIfAbsent(name, check) != null) throw new IllegalStateException("check already registered: " + name);
        if (checks.size() > maxChecks) {
            checks.remove(name, check);
            throw new IllegalStateException("health check capacity exceeded");
        }
    }

    public boolean remove(String name) {
        requireText(name, "name");
        return checks.remove(name) != null;
    }

    public int size() { return checks.size(); }

    public CompletionStage<Snapshot> check(Duration timeout, long nowMillis) {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) throw new IllegalArgumentException("timeout must be positive");
        if (nowMillis < 0) throw new IllegalArgumentException("nowMillis must not be negative");
        List<Map.Entry<String, Check>> entries = checks.entrySet().stream()
                .sorted(Map.Entry.comparingByKey()).toList();
        List<CompletableFuture<ComponentStatus>> futures = entries.stream()
                .map(entry -> evaluate(entry.getKey(), entry.getValue(), timeout))
                .toList();
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> {
                    List<ComponentStatus> statuses = futures.stream().map(CompletableFuture::join).toList();
                    return new Snapshot(nowMillis, overall(statuses), statuses);
                });
    }

    private static CompletableFuture<ComponentStatus> evaluate(String name, Check check, Duration timeout) {
        long started = System.nanoTime();
        CompletionStage<State> stage;
        try {
            stage = Objects.requireNonNull(check.run(), "health check returned null");
        } catch (Throwable failure) {
            return CompletableFuture.completedFuture(failed(name, started, "exception: " + message(failure)));
        }
        return stage.toCompletableFuture()
                .orTimeout(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                .handle((state, error) -> {
                    long latency = elapsedMillis(started);
                    if (error != null) return new ComponentStatus(name, State.UNKNOWN,
                            "timeout/error: " + message(error), latency);
                    if (state == null) return new ComponentStatus(name, State.UNKNOWN, "null state", latency);
                    return new ComponentStatus(name, state, state == State.UP ? "ok" : state.name().toLowerCase(), latency);
                });
    }

    private static ComponentStatus failed(String name, long started, String detail) {
        return new ComponentStatus(name, State.UNKNOWN, detail.substring(0, Math.min(detail.length(), 512)),
                elapsedMillis(started));
    }

    private static State overall(List<ComponentStatus> statuses) {
        if (statuses.isEmpty()) return State.UNKNOWN;
        boolean degraded = false;
        for (ComponentStatus status : statuses) {
            if (status.state() == State.DOWN) return State.DOWN;
            if (status.state() != State.UP) degraded = true;
        }
        return degraded ? State.DEGRADED : State.UP;
    }

    private static long elapsedMillis(long started) {
        return Math.max(0, Duration.ofNanos(System.nanoTime() - started).toMillis());
    }

    private static String message(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) cause = cause.getCause();
        String message = cause.getMessage();
        return message == null || message.isBlank() ? cause.getClass().getSimpleName() : message;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
}

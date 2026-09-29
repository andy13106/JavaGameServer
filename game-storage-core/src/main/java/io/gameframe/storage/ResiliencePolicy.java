package io.gameframe.storage;

import java.time.Duration;
import java.util.Objects;

/** Finite retry, timeout and circuit-breaker limits for asynchronous storage operations. */
public record ResiliencePolicy(
    int maxAttempts,
    Duration attemptTimeout,
    Duration initialBackoff,
    Duration maxBackoff,
    int circuitFailureThreshold,
    Duration circuitOpenDuration) {

    public ResiliencePolicy {
        if (maxAttempts < 1 || circuitFailureThreshold < 1) throw new IllegalArgumentException();
        requirePositive(attemptTimeout, "attemptTimeout");
        requireNonNegative(initialBackoff, "initialBackoff");
        requireNonNegative(maxBackoff, "maxBackoff");
        requirePositive(circuitOpenDuration, "circuitOpenDuration");
        if (initialBackoff.compareTo(maxBackoff) > 0)
            throw new IllegalArgumentException("initialBackoff exceeds maxBackoff");
    }

    public static ResiliencePolicy defaults() {
        return new ResiliencePolicy(3, Duration.ofSeconds(2), Duration.ofMillis(50),
            Duration.ofSeconds(1), 5, Duration.ofSeconds(5));
    }

    long backoffNanos(int completedAttempt) {
        long value = initialBackoff.toNanos();
        long cap = maxBackoff.toNanos();
        for (int i = 1; i < completedAttempt && value < cap; i++)
            value = value > cap / 2 ? cap : value * 2;
        return Math.min(value, cap);
    }

    private static void requirePositive(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isZero() || value.isNegative()) throw new IllegalArgumentException(name + " must be positive");
        value.toNanos();
    }
    private static void requireNonNegative(Duration value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isNegative()) throw new IllegalArgumentException(name + " must not be negative");
        value.toNanos();
    }
}
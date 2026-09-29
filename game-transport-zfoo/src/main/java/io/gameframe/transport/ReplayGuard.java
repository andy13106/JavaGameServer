package io.gameframe.transport;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Bounded replay protection for authenticated request envelopes.
 * Callers must invoke this after authenticating the identity and before applying side effects.
 */
public final class ReplayGuard {
    public enum Rejection {
        EXPIRED,
        FUTURE,
        DUPLICATE,
        CAPACITY
    }

    public record Limits(int maxEntries, Duration pastWindow, Duration futureSkew, Duration nonceTtl) {
        public Limits {
            if (maxEntries < 1) throw new IllegalArgumentException("maxEntries must be positive");
            Objects.requireNonNull(pastWindow, "pastWindow");
            Objects.requireNonNull(futureSkew, "futureSkew");
            Objects.requireNonNull(nonceTtl, "nonceTtl");
            if (pastWindow.isNegative() || pastWindow.isZero()) {
                throw new IllegalArgumentException("pastWindow must be positive");
            }
            if (futureSkew.isNegative()) throw new IllegalArgumentException("futureSkew cannot be negative");
            if (nonceTtl.isNegative() || nonceTtl.isZero()) {
                throw new IllegalArgumentException("nonceTtl must be positive");
            }
        }

        public static Limits defaults() {
            return new Limits(100_000, Duration.ofSeconds(30), Duration.ofSeconds(5), Duration.ofMinutes(2));
        }
    }

    public record Decision(boolean accepted, Rejection rejection) {
        public static Decision allow() {
            return new Decision(true, null);
        }

        public static Decision rejected(Rejection rejection) {
            return new Decision(false, Objects.requireNonNull(rejection, "rejection"));
        }
    }

    private record Key(String identity, String nonce) {
    }

    private final Limits limits;
    private final LongSupplier clockMillis;
    private final Map<Key, Long> seen = new HashMap<>();

    public ReplayGuard(Limits limits) {
        this(limits, System::currentTimeMillis);
    }

    ReplayGuard(Limits limits, LongSupplier clockMillis) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.clockMillis = Objects.requireNonNull(clockMillis, "clockMillis");
    }

    public Limits limits() {
        return limits;
    }

    public synchronized Decision accept(String identity, long timestampMillis, String nonce) {
        if (identity == null || identity.isBlank()) {
            throw new IllegalArgumentException("identity must not be blank");
        }
        if (nonce == null || nonce.isBlank()) {
            throw new IllegalArgumentException("nonce must not be blank");
        }
        long now = clockMillis.getAsLong();
        if (timestampMillis < now - limits.pastWindow().toMillis()) {
            return Decision.rejected(Rejection.EXPIRED);
        }
        if (timestampMillis > now + limits.futureSkew().toMillis()) {
            return Decision.rejected(Rejection.FUTURE);
        }
        purgeExpired(now);
        Key key = new Key(identity, nonce);
        if (seen.containsKey(key)) {
            return Decision.rejected(Rejection.DUPLICATE);
        }
        if (seen.size() >= limits.maxEntries()) {
            return Decision.rejected(Rejection.CAPACITY);
        }
        seen.put(key, safeAdd(now, limits.nonceTtl().toMillis()));
        return Decision.allow();
    }

    public synchronized int size() {
        purgeExpired(clockMillis.getAsLong());
        return seen.size();
    }

    public synchronized int purge() {
        return purgeExpired(clockMillis.getAsLong());
    }

    private int purgeExpired(long now) {
        int before = seen.size();
        seen.entrySet().removeIf(entry -> entry.getValue() <= now);
        return before - seen.size();
    }

    private static long safeAdd(long left, long right) {
        if (right > 0 && left > Long.MAX_VALUE - right) return Long.MAX_VALUE;
        return left + right;
    }
}

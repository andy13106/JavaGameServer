package io.gameframe.transport;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplayGuardTest {
    @Test
    void scopesNonceByIdentityAndRejectsDuplicate() {
        var clock = new AtomicLong(1_000);
        var guard = new ReplayGuard(limits(8), clock::get);

        assertTrue(guard.accept("uid-1", 1_000, "nonce-1").accepted());
        assertEquals(ReplayGuard.Rejection.DUPLICATE,
                guard.accept("uid-1", 1_000, "nonce-1").rejection());
        assertTrue(guard.accept("uid-2", 1_000, "nonce-1").accepted());
        assertEquals(2, guard.size());
    }

    @Test
    void rejectsPastAndFutureClockSkew() {
        var clock = new AtomicLong(10_000);
        var guard = new ReplayGuard(new ReplayGuard.Limits(
                8, Duration.ofSeconds(5), Duration.ofSeconds(2), Duration.ofSeconds(20)), clock::get);

        assertEquals(ReplayGuard.Rejection.EXPIRED,
                guard.accept("uid", 4_999, "old").rejection());
        assertEquals(ReplayGuard.Rejection.FUTURE,
                guard.accept("uid", 12_001, "future").rejection());
        assertTrue(guard.accept("uid", 5_000, "edge-past").accepted());
        assertTrue(guard.accept("uid", 12_000, "edge-future").accepted());
    }

    @Test
    void expiresEntriesAndAllowsReuse() {
        var clock = new AtomicLong(1_000);
        var guard = new ReplayGuard(new ReplayGuard.Limits(
                8, Duration.ofSeconds(10), Duration.ZERO, Duration.ofSeconds(5)), clock::get);

        assertTrue(guard.accept("uid", 1_000, "nonce").accepted());
        clock.set(6_000);
        assertEquals(1, guard.purge());
        assertEquals(0, guard.size());
        assertTrue(guard.accept("uid", 6_000, "nonce").accepted());
    }

    @Test
    void rejectsNewEntriesAtCapacityAfterPurgingExpiredEntries() {
        var clock = new AtomicLong(1_000);
        var guard = new ReplayGuard(new ReplayGuard.Limits(
                2, Duration.ofSeconds(10), Duration.ZERO, Duration.ofSeconds(20)), clock::get);

        assertTrue(guard.accept("uid", 1_000, "one").accepted());
        assertTrue(guard.accept("uid", 1_000, "two").accepted());
        assertEquals(ReplayGuard.Rejection.CAPACITY,
                guard.accept("uid", 1_000, "three").rejection());
        assertFalse(guard.accept("uid", 1_000, "one").accepted());
        clock.set(21_000);
        assertTrue(guard.accept("uid", 21_000, "three").accepted());
    }

    @Test
    void validatesIdentityNonceAndLimits() {
        var guard = new ReplayGuard(limits(2));
        assertThrows(IllegalArgumentException.class, () -> guard.accept("", 0, "nonce"));
        assertThrows(IllegalArgumentException.class, () -> guard.accept("uid", 0, ""));
        assertThrows(IllegalArgumentException.class, () -> new ReplayGuard.Limits(
                0, Duration.ofSeconds(1), Duration.ZERO, Duration.ofSeconds(1)));
    }

    private static ReplayGuard.Limits limits(int capacity) {
        return new ReplayGuard.Limits(capacity, Duration.ofSeconds(10), Duration.ZERO, Duration.ofSeconds(20));
    }
}

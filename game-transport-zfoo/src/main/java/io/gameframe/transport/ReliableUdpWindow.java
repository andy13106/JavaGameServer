package io.gameframe.transport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ReliableUdpWindow<T> {
    public static final long MAX_SEQUENCE = 0xffff_ffffL;
    private static final long HALF_SEQUENCE_SPACE = 0x8000_0000L;

    public record Limits(int maxPendingPackets, long maxPendingBytes, Duration resendAfter, int maxAttempts, int reorderWindow) {
        public Limits {
            if (maxPendingPackets < 1 || maxPendingBytes < 1 || resendAfter.isZero() || resendAfter.isNegative()
                    || maxAttempts < 1 || reorderWindow < 1) {
                throw new IllegalArgumentException("invalid reliable UDP limits");
            }
        }
    }

    public record Outbound<T>(long sequence, T payload, int bytes, int attempts) {}
    public record Received<T>(Kind kind, List<Sequenced<T>> deliveries) {
        public Received { deliveries = List.copyOf(deliveries); }
    }
    public record Sequenced<T>(long sequence, T payload) {}
    public enum Kind { DELIVERED, BUFFERED, DUPLICATE, REJECTED }
    public record Snapshot(long pendingPackets, long pendingBytes, long nextSequence, long nextExpected,
                           long bufferedPackets, long acknowledged, long retransmitted, long expired,
                           long duplicates, long rejected) {}

    private final Limits limits;
    private final Map<Long, Pending<T>> pending = new LinkedHashMap<>();
    private final Map<Long, Sequenced<T>> reorder = new HashMap<>();
    private long nextSequence;
    private long nextExpected;
    private long pendingBytes;
    private long acknowledged;
    private long retransmitted;
    private long expired;
    private long duplicates;
    private long rejected;

    public ReliableUdpWindow(Limits limits) {
        this(limits, 1);
    }

    public ReliableUdpWindow(Limits limits, long initialSequence) {
        this.limits = Objects.requireNonNull(limits, "limits");
        requireSequence(initialSequence);
        nextSequence = initialSequence;
        nextExpected = initialSequence;
    }

    public synchronized Outbound<T> offer(T payload, int bytes, long nowNanos) {
        Objects.requireNonNull(payload, "payload");
        if (bytes < 1 || pending.size() >= limits.maxPendingPackets()
                || bytes > limits.maxPendingBytes() - pendingBytes
                || pending.containsKey(nextSequence)) {
            rejected++;
            return null;
        }
        var sequence = nextSequence;
        nextSequence = increment(nextSequence);
        var value = new Pending<>(sequence, payload, bytes, 1, nowNanos + limits.resendAfter().toNanos());
        pending.put(sequence, value);
        pendingBytes += bytes;
        return new Outbound<>(sequence, payload, bytes, 1);
    }

    public synchronized boolean acknowledge(long sequence) {
        if (!isSequence(sequence)) return false;
        var value = pending.remove(sequence);
        if (value == null) return false;
        pendingBytes -= value.bytes;
        acknowledged++;
        return true;
    }

    public synchronized List<Outbound<T>> due(long nowNanos) {
        var result = new ArrayList<Outbound<T>>();
        var iterator = pending.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            var value = entry.getValue();
            if (nowNanos < value.nextSendNanos) continue;
            if (value.attempts >= limits.maxAttempts()) {
                pendingBytes -= value.bytes;
                iterator.remove();
                expired++;
                continue;
            }
            value.attempts++;
            value.nextSendNanos = nowNanos + limits.resendAfter().toNanos();
            retransmitted++;
            result.add(new Outbound<>(value.sequence, value.payload, value.bytes, value.attempts));
        }
        return List.copyOf(result);
    }

    public synchronized Received<T> receive(long sequence, T payload) {
        Objects.requireNonNull(payload, "payload");
        if (!isSequence(sequence)) {
            rejected++;
            return new Received<>(Kind.REJECTED, List.of());
        }
        long distance = forwardDistance(nextExpected, sequence);
        if (distance >= HALF_SEQUENCE_SPACE || reorder.containsKey(sequence)) {
            duplicates++;
            return new Received<>(Kind.DUPLICATE, List.of());
        }
        if (distance > limits.reorderWindow()) {
            rejected++;
            return new Received<>(Kind.REJECTED, List.of());
        }
        reorder.put(sequence, new Sequenced<>(sequence, payload));
        if (sequence != nextExpected) return new Received<>(Kind.BUFFERED, List.of());

        var deliveries = new ArrayList<Sequenced<T>>();
        while (true) {
            var next = reorder.remove(nextExpected);
            if (next == null) break;
            deliveries.add(next);
            nextExpected = increment(nextExpected);
        }
        return new Received<>(Kind.DELIVERED, deliveries);
    }

    public synchronized Snapshot snapshot() {
        return new Snapshot(pending.size(), pendingBytes, nextSequence, nextExpected, reorder.size(),
                acknowledged, retransmitted, expired, duplicates, rejected);
    }

    private static long increment(long sequence) {
        return (sequence + 1) & MAX_SEQUENCE;
    }

    private static long forwardDistance(long from, long to) {
        return (to - from) & MAX_SEQUENCE;
    }

    private static boolean isSequence(long sequence) {
        return sequence >= 0 && sequence <= MAX_SEQUENCE;
    }

    private static void requireSequence(long sequence) {
        if (!isSequence(sequence)) throw new IllegalArgumentException("sequence must be an unsigned 32-bit value");
    }

    private static final class Pending<T> {
        private final long sequence;
        private final T payload;
        private final int bytes;
        private int attempts;
        private long nextSendNanos;
        private Pending(long sequence, T payload, int bytes, int attempts, long nextSendNanos) {
            this.sequence = sequence; this.payload = payload; this.bytes = bytes;
            this.attempts = attempts; this.nextSendNanos = nextSendNanos;
        }
    }
}

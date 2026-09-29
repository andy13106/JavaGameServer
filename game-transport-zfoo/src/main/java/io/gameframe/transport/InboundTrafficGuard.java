package io.gameframe.transport;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/** Per-channel inbound byte, packet and decoded-activity accounting. */
public final class InboundTrafficGuard {
    public enum Rejection {
        PACKET_RATE,
        BYTE_RATE,
        HEARTBEAT_TIMEOUT
    }

    public record Limits(int maxPackets, long maxBytes, Duration window, Duration heartbeatTimeout) {
        public Limits {
            if (maxPackets < 1 || maxBytes < 1) throw new IllegalArgumentException("positive limits required");
            Objects.requireNonNull(window);
            Objects.requireNonNull(heartbeatTimeout);
            if (window.isZero() || window.isNegative()) throw new IllegalArgumentException("positive window required");
            if (heartbeatTimeout.isZero() || heartbeatTimeout.isNegative())
                throw new IllegalArgumentException("positive heartbeat timeout required");
        }
    }

    public record Snapshot(int windowPackets, long windowBytes,
                           long acceptedPackets, long acceptedBytes,
                           long packetRateRejections, long byteRateRejections,
                           long heartbeatTimeouts, long lastDecodedActivityNanos) {}

    private static final AtomicLong IDS = new AtomicLong();

    private final Limits limits;
    private final LongSupplier clock;
    private final long windowNanos;
    private final long heartbeatTimeoutNanos;
    private final AttributeKey<State> stateKey =
        AttributeKey.valueOf("gameframe.inboundTraffic." + IDS.incrementAndGet());

    public InboundTrafficGuard(Limits limits) {
        this(limits, System::nanoTime);
    }

    InboundTrafficGuard(Limits limits, LongSupplier clock) {
        this.limits = Objects.requireNonNull(limits);
        this.clock = Objects.requireNonNull(clock);
        this.windowNanos = limits.window().toNanos();
        this.heartbeatTimeoutNanos = limits.heartbeatTimeout().toNanos();
    }

    public Limits limits() {
        return limits;
    }

    public Snapshot snapshot(Channel channel) {
        return state(channel, clock.getAsLong()).snapshot();
    }

    long nowNanos() {
        return clock.getAsLong();
    }

    boolean admitBytes(Channel channel, int bytes, long nowNanos) {
        if (bytes < 0) throw new IllegalArgumentException("bytes cannot be negative");
        return state(channel, nowNanos).admitBytes(bytes, nowNanos);
    }

    boolean admitPacket(Channel channel, long nowNanos) {
        return state(channel, nowNanos).admitPacket(nowNanos);
    }

    long nanosUntilHeartbeatTimeout(Channel channel, long nowNanos) {
        return state(channel, nowNanos).nanosUntilHeartbeatTimeout(nowNanos);
    }

    boolean markHeartbeatTimeout(Channel channel, long nowNanos) {
        return state(channel, nowNanos).markHeartbeatTimeout(nowNanos);
    }

    private State state(Channel channel, long nowNanos) {
        Objects.requireNonNull(channel);
        var attribute = channel.attr(stateKey);
        State existing = attribute.get();
        if (existing != null) return existing;
        State created = new State(nowNanos);
        State prior = attribute.setIfAbsent(created);
        return prior == null ? created : prior;
    }

    private final class State {
        private long windowStartNanos;
        private int windowPackets;
        private long windowBytes;
        private long acceptedPackets;
        private long acceptedBytes;
        private long packetRateRejections;
        private long byteRateRejections;
        private long heartbeatTimeouts;
        private long lastDecodedActivityNanos;
        private boolean heartbeatTimedOut;

        private State(long nowNanos) {
            windowStartNanos = nowNanos;
            lastDecodedActivityNanos = nowNanos;
        }

        private synchronized boolean admitBytes(int bytes, long nowNanos) {
            rotateWindow(nowNanos);
            if (bytes > limits.maxBytes() - windowBytes) {
                byteRateRejections++;
                return false;
            }
            windowBytes += bytes;
            acceptedBytes += bytes;
            return true;
        }

        private synchronized boolean admitPacket(long nowNanos) {
            rotateWindow(nowNanos);
            if (windowPackets >= limits.maxPackets()) {
                packetRateRejections++;
                return false;
            }
            windowPackets++;
            acceptedPackets++;
            lastDecodedActivityNanos = nowNanos;
            heartbeatTimedOut = false;
            return true;
        }

        private synchronized long nanosUntilHeartbeatTimeout(long nowNanos) {
            long elapsed = Math.max(0, nowNanos - lastDecodedActivityNanos);
            return Math.max(0, heartbeatTimeoutNanos - elapsed);
        }

        private synchronized boolean markHeartbeatTimeout(long nowNanos) {
            if (heartbeatTimedOut || nanosUntilHeartbeatTimeout(nowNanos) > 0) return false;
            heartbeatTimedOut = true;
            heartbeatTimeouts++;
            return true;
        }

        private synchronized Snapshot snapshot() {
            return new Snapshot(windowPackets, windowBytes, acceptedPackets, acceptedBytes,
                packetRateRejections, byteRateRejections, heartbeatTimeouts, lastDecodedActivityNanos);
        }

        private void rotateWindow(long nowNanos) {
            if (nowNanos - windowStartNanos < windowNanos) return;
            windowStartNanos = nowNanos;
            windowPackets = 0;
            windowBytes = 0;
        }
    }
}


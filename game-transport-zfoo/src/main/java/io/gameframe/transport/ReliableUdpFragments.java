package io.gameframe.transport;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ReliableUdpFragments {
    private ReliableUdpFragments() {}

    public static List<byte[]> split(byte[] payload, int maxFragmentBytes) {
        Objects.requireNonNull(payload, "payload");
        if (maxFragmentBytes < 1) throw new IllegalArgumentException("maxFragmentBytes must be positive");
        long countValue = Math.max(1L, (payload.length + (long) maxFragmentBytes - 1) / maxFragmentBytes);
        if (countValue > 0xffff) throw new IllegalArgumentException("message requires too many UDP fragments");
        int count = (int) countValue;
        var fragments = new ArrayList<byte[]>(count);
        for (int index = 0; index < count; index++) {
            int from = index * maxFragmentBytes;
            int to = Math.min(payload.length, from + maxFragmentBytes);
            fragments.add(Arrays.copyOfRange(payload, from, to));
        }
        return List.copyOf(fragments);
    }

    public static final class Reassembler {
        public record Limits(int maxMessages, long maxBytes, Duration timeout) {
            public Limits {
                Objects.requireNonNull(timeout, "timeout");
                if (maxMessages < 1 || maxBytes < 1 || maxBytes > Integer.MAX_VALUE
                        || timeout.isZero() || timeout.isNegative()) {
                    throw new IllegalArgumentException("invalid reliable UDP reassembly limits");
                }
            }
        }

        public enum Status { BUFFERED, COMPLETE, DUPLICATE, REJECTED }
        public record Result(Status status, byte[] payload) {
            public Result {
                Objects.requireNonNull(status, "status");
                payload = payload == null ? null : payload.clone();
            }

            @Override
            public byte[] payload() {
                return payload == null ? null : payload.clone();
            }
        }

        private final Limits limits;
        private final Map<Key, Assembly> assemblies = new HashMap<>();
        private long bufferedBytes;

        public Reassembler(Limits limits) {
            this.limits = Objects.requireNonNull(limits, "limits");
        }

        public synchronized Result accept(ReliableUdpFrame frame, long nowNanos) {
            Objects.requireNonNull(frame, "frame");
            if (frame.kind() != ReliableUdpFrame.Kind.DATA) {
                return new Result(Status.REJECTED, null);
            }
            expire(nowNanos);
            var key = new Key(frame.sessionGeneration(), frame.messageId());
            var assembly = assemblies.get(key);
            if (assembly == null) {
                if (assemblies.size() >= limits.maxMessages()) return new Result(Status.REJECTED, null);
                assembly = new Assembly(frame.fragmentCount(), nowNanos + limits.timeout().toNanos());
                assemblies.put(key, assembly);
            } else if (assembly.fragments.length != frame.fragmentCount()) {
                remove(key, assembly);
                return new Result(Status.REJECTED, null);
            }

            int index = frame.fragmentIndex();
            if (assembly.fragments[index] != null) return new Result(Status.DUPLICATE, null);
            byte[] payload = frame.payload();
            if (payload.length > limits.maxBytes() - bufferedBytes) {
                remove(key, assembly);
                return new Result(Status.REJECTED, null);
            }
            assembly.fragments[index] = payload;
            assembly.received++;
            assembly.bytes += payload.length;
            bufferedBytes += payload.length;
            if (assembly.received != assembly.fragments.length) return new Result(Status.BUFFERED, null);

            if (assembly.bytes > Integer.MAX_VALUE) {
                remove(key, assembly);
                return new Result(Status.REJECTED, null);
            }
            byte[] complete = new byte[(int) assembly.bytes];
            int offset = 0;
            for (byte[] fragment : assembly.fragments) {
                System.arraycopy(fragment, 0, complete, offset, fragment.length);
                offset += fragment.length;
            }
            remove(key, assembly);
            return new Result(Status.COMPLETE, complete);
        }

        public synchronized int expire(long nowNanos) {
            int expired = 0;
            var iterator = assemblies.entrySet().iterator();
            while (iterator.hasNext()) {
                var entry = iterator.next();
                if (nowNanos < entry.getValue().expiresAtNanos) continue;
                bufferedBytes -= entry.getValue().bytes;
                iterator.remove();
                expired++;
            }
            return expired;
        }

        public synchronized int pendingMessages() {
            return assemblies.size();
        }

        public synchronized long bufferedBytes() {
            return bufferedBytes;
        }

        private void remove(Key key, Assembly assembly) {
            assemblies.remove(key);
            bufferedBytes -= assembly.bytes;
        }

        private record Key(long generation, long messageId) {}

        private static final class Assembly {
            private final byte[][] fragments;
            private final long expiresAtNanos;
            private int received;
            private long bytes;

            private Assembly(int fragmentCount, long expiresAtNanos) {
                fragments = new byte[fragmentCount][];
                this.expiresAtNanos = expiresAtNanos;
            }
        }
    }
}

package io.gameframe.transport;

import java.util.Objects;

/** A versioned reliable-UDP envelope independent of the zfoo packet body. */
public record ReliableUdpFrame(Kind kind, long sessionGeneration, long sequence, long messageId,
                               int fragmentIndex, int fragmentCount, byte[] payload) {
    public enum Kind {
        DATA(1), ACK(2), HELLO(3);

        private final int wireValue;

        Kind(int wireValue) {
            this.wireValue = wireValue;
        }

        int wireValue() {
            return wireValue;
        }

        static Kind fromWireValue(int value) {
            return switch (value) {
                case 1 -> DATA;
                case 2 -> ACK;
                case 3 -> HELLO;
                default -> throw new IllegalArgumentException("unknown reliable UDP frame kind: " + value);
            };
        }
    }

    public ReliableUdpFrame {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(payload, "payload");
        requireUnsignedInt(sequence, "sequence");
        requireUnsignedInt(messageId, "messageId");
        payload = payload.clone();
        if (kind == Kind.ACK || kind == Kind.HELLO) {
            if (messageId != 0 || fragmentIndex != 0 || fragmentCount != 0 || payload.length != 0) {
                throw new IllegalArgumentException("control frame cannot contain message or payload fields");
            }
        } else if (fragmentCount < 1 || fragmentCount > 0xffff
                || fragmentIndex < 0 || fragmentIndex >= fragmentCount) {
            throw new IllegalArgumentException("invalid reliable UDP fragment coordinates");
        }
    }

    public static ReliableUdpFrame data(long generation, long sequence, long messageId,
                                        int fragmentIndex, int fragmentCount, byte[] payload) {
        return new ReliableUdpFrame(Kind.DATA, generation, sequence, messageId,
                fragmentIndex, fragmentCount, payload);
    }

    public static ReliableUdpFrame acknowledgement(long generation, long sequence) {
        return new ReliableUdpFrame(Kind.ACK, generation, sequence, 0, 0, 0, new byte[0]);
    }

    public static ReliableUdpFrame hello(long generation) {
        return new ReliableUdpFrame(Kind.HELLO, generation, 0, 0, 0, 0, new byte[0]);
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    private static void requireUnsignedInt(long value, String name) {
        if (value < 0 || value > ReliableUdpWindow.MAX_SEQUENCE) {
            throw new IllegalArgumentException(name + " must be an unsigned 32-bit value");
        }
    }
}

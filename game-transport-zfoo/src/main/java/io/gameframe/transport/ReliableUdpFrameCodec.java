package io.gameframe.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Objects;
import java.util.zip.CRC32C;

/** Strict binary codec for reliable UDP frames. */
public final class ReliableUdpFrameCodec {
    public static final int MAGIC = 0x47465550; // GFUP
    public static final int VERSION = 1;
    public static final int FRAME_OVERHEAD = 34;
    public static final int MAX_UDP_PAYLOAD = 65_507;
    public static final int MAX_FRAME_PAYLOAD = MAX_UDP_PAYLOAD - FRAME_OVERHEAD;

    private ReliableUdpFrameCodec() {}

    public static ByteBuf encode(ByteBufAllocator allocator, ReliableUdpFrame frame) {
        Objects.requireNonNull(allocator, "allocator");
        Objects.requireNonNull(frame, "frame");
        byte[] payload = frame.payload();
        if (payload.length > MAX_FRAME_PAYLOAD) {
            throw new IllegalArgumentException("reliable UDP payload exceeds datagram limit");
        }

        ByteBuf out = allocator.buffer(FRAME_OVERHEAD + payload.length);
        int checksumStart = out.writerIndex();
        out.writeInt(MAGIC);
        out.writeByte(VERSION);
        out.writeByte(frame.kind().wireValue());
        out.writeLong(frame.sessionGeneration());
        out.writeInt((int) frame.sequence());
        out.writeInt((int) frame.messageId());
        out.writeShort(frame.fragmentIndex());
        out.writeShort(frame.fragmentCount());
        out.writeInt(payload.length);
        out.writeBytes(payload);
        out.writeInt(checksum(out, checksumStart, out.writerIndex() - checksumStart));
        return out;
    }

    public static ReliableUdpFrame decode(ByteBuf source) {
        return decode(source, MAX_FRAME_PAYLOAD);
    }

    public static ReliableUdpFrame decode(ByteBuf source, int maxPayloadBytes) {
        Objects.requireNonNull(source, "source");
        if (maxPayloadBytes < 0 || maxPayloadBytes > MAX_FRAME_PAYLOAD) {
            throw new IllegalArgumentException("invalid reliable UDP payload limit");
        }
        if (source.readableBytes() < FRAME_OVERHEAD) {
            throw new IllegalArgumentException("truncated reliable UDP frame");
        }

        ByteBuf in = source.duplicate();
        int frameStart = in.readerIndex();
        if (in.readInt() != MAGIC) throw new IllegalArgumentException("invalid reliable UDP magic");
        int version = in.readUnsignedByte();
        if (version != VERSION) throw new IllegalArgumentException("unsupported reliable UDP version: " + version);
        var kind = ReliableUdpFrame.Kind.fromWireValue(in.readUnsignedByte());
        long generation = in.readLong();
        long sequence = in.readUnsignedInt();
        long messageId = in.readUnsignedInt();
        int fragmentIndex = in.readUnsignedShort();
        int fragmentCount = in.readUnsignedShort();
        int payloadLength = in.readInt();
        if (payloadLength < 0 || payloadLength > maxPayloadBytes) {
            throw new IllegalArgumentException("invalid reliable UDP payload length: " + payloadLength);
        }
        if (in.readableBytes() != payloadLength + Integer.BYTES) {
            throw new IllegalArgumentException("reliable UDP frame length mismatch");
        }
        byte[] payload = new byte[payloadLength];
        in.readBytes(payload);
        long expectedChecksum = in.readUnsignedInt();
        long actualChecksum = Integer.toUnsignedLong(checksum(source, frameStart,
                source.readableBytes() - Integer.BYTES));
        if (expectedChecksum != actualChecksum) {
            throw new IllegalArgumentException("reliable UDP checksum mismatch");
        }
        return new ReliableUdpFrame(kind, generation, sequence, messageId,
                fragmentIndex, fragmentCount, payload);
    }

    private static int checksum(ByteBuf buffer, int index, int length) {
        byte[] bytes = new byte[length];
        buffer.getBytes(index, bytes);
        var checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        return (int) checksum.getValue();
    }
}

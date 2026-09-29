package io.gameframe.transport;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class ReliableUdpFrameCodecTest {
    @Test
    void dataFrameRoundTripsWithoutChangingSourceReaderIndex() {
        var frame = ReliableUdpFrame.data(7, 0xffff_ffffL, 12, 1, 3,
                "payload".getBytes(StandardCharsets.UTF_8));
        var encoded = ReliableUdpFrameCodec.encode(UnpooledByteBufAllocator.DEFAULT, frame);
        try {
            int readerIndex = encoded.readerIndex();
            var decoded = ReliableUdpFrameCodec.decode(encoded);
            assertEquals(readerIndex, encoded.readerIndex());
            assertEquals(frame.kind(), decoded.kind());
            assertEquals(frame.sessionGeneration(), decoded.sessionGeneration());
            assertEquals(frame.sequence(), decoded.sequence());
            assertEquals(frame.messageId(), decoded.messageId());
            assertEquals(frame.fragmentIndex(), decoded.fragmentIndex());
            assertEquals(frame.fragmentCount(), decoded.fragmentCount());
            assertArrayEquals(frame.payload(), decoded.payload());
        } finally {
            encoded.release();
        }
    }

    @Test
    void acknowledgementRoundTrips() {
        var encoded = ReliableUdpFrameCodec.encode(UnpooledByteBufAllocator.DEFAULT,
                ReliableUdpFrame.acknowledgement(99, 42));
        try {
            var decoded = ReliableUdpFrameCodec.decode(encoded);
            assertEquals(ReliableUdpFrame.Kind.ACK, decoded.kind());
            assertEquals(42, decoded.sequence());
            assertEquals(0, decoded.payload().length);
        } finally {
            encoded.release();
        }
    }

    @Test
    void corruptedAndTrailingBytesAreRejected() {
        var encoded = ReliableUdpFrameCodec.encode(UnpooledByteBufAllocator.DEFAULT,
                ReliableUdpFrame.data(1, 1, 1, 0, 1, new byte[]{1, 2, 3}));
        try {
            encoded.setByte(ReliableUdpFrameCodec.FRAME_OVERHEAD - Integer.BYTES, 9);
            assertThrows(IllegalArgumentException.class, () -> ReliableUdpFrameCodec.decode(encoded));
        } finally {
            encoded.release();
        }

        var withTrailingByte = UnpooledByteBufAllocator.DEFAULT.buffer();
        var valid = ReliableUdpFrameCodec.encode(UnpooledByteBufAllocator.DEFAULT,
                ReliableUdpFrame.acknowledgement(1, 1));
        try {
            withTrailingByte.writeBytes(valid).writeByte(0);
            assertThrows(IllegalArgumentException.class, () -> ReliableUdpFrameCodec.decode(withTrailingByte));
        } finally {
            valid.release();
            withTrailingByte.release();
        }
    }

    @Test
    void malformedAckIsRejectedByFrameValidation() {
        assertThrows(IllegalArgumentException.class, () -> new ReliableUdpFrame(
                ReliableUdpFrame.Kind.ACK, 1, 1, 1, 0, 0, new byte[0]));
    }
}

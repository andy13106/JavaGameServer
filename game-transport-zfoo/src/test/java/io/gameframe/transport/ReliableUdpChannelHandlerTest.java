package io.gameframe.transport;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DatagramPacket;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class ReliableUdpChannelHandlerTest {
    private static final InetSocketAddress LOCAL = new InetSocketAddress("127.0.0.1", 9000);
    private static final InetSocketAddress REMOTE = new InetSocketAddress("127.0.0.1", 9001);

    private static ReliableUdpChannelHandler.Limits limits(int fragmentBytes, int pendingPackets) {
        return new ReliableUdpChannelHandler.Limits(
                4, fragmentBytes, Duration.ofSeconds(30), Duration.ofMillis(10),
                new ReliableUdpWindow.Limits(pendingPackets, 1024, Duration.ofMillis(50), 3, 8),
                new ReliableUdpFragments.Reassembler.Limits(4, 1024, Duration.ofSeconds(1)));
    }

    @Test
    void outboundDatagramIsFragmentedAndAcknowledgementReleasesWindow() {
        var clock = new AtomicLong();
        var handler = new ReliableUdpChannelHandler(limits(2, 3), 77, clock::get);
        var channel = new EmbeddedChannel(handler);
        try {
            assertTrue(channel.writeOutbound(new DatagramPacket(
                    Unpooled.wrappedBuffer(new byte[]{1, 2, 3, 4, 5}), REMOTE)));

            DatagramPacket hello = channel.readOutbound();
            assertEquals(ReliableUdpFrame.Kind.HELLO, ReliableUdpFrameCodec.decode(hello.content()).kind());
            hello.release();

            for (int index = 0; index < 3; index++) {
                DatagramPacket wire = channel.readOutbound();
                var frame = ReliableUdpFrameCodec.decode(wire.content());
                assertEquals(ReliableUdpFrame.Kind.DATA, frame.kind());
                assertEquals(77, frame.sessionGeneration());
                assertEquals(index + 1, frame.sequence());
                assertEquals(1, frame.messageId());
                assertEquals(index, frame.fragmentIndex());
                assertEquals(3, frame.fragmentCount());
                wire.release();
            }

            assertThrows(io.netty.handler.codec.EncoderException.class, () -> channel.writeOutbound(
                    new DatagramPacket(Unpooled.wrappedBuffer(new byte[]{9}), REMOTE)));
            var ack = ReliableUdpFrameCodec.encode(channel.alloc(), ReliableUdpFrame.acknowledgement(77, 1));
            assertFalse(channel.writeInbound(new DatagramPacket(ack, LOCAL, REMOTE)));
            assertTrue(channel.writeOutbound(new DatagramPacket(Unpooled.wrappedBuffer(new byte[]{9}), REMOTE)));
            ((DatagramPacket) channel.readOutbound()).release();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void outOfOrderFragmentsAreAcknowledgedAndDeliveredInOrder() {
        var handler = new ReliableUdpChannelHandler(limits(2, 8), 77, System::nanoTime);
        var channel = new EmbeddedChannel(handler);
        try {
            assertFalse(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(90, 2, 5, 1, 2, new byte[]{3, 4})));
            DatagramPacket ack2 = channel.readOutbound();
            assertEquals(2, ReliableUdpFrameCodec.decode(ack2.content()).sequence());
            ack2.release();
            DatagramPacket hello = channel.readOutbound();
            assertEquals(ReliableUdpFrame.Kind.HELLO, ReliableUdpFrameCodec.decode(hello.content()).kind());
            hello.release();

            assertTrue(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(90, 1, 5, 0, 2, new byte[]{1, 2})));
            DatagramPacket ack1 = channel.readOutbound();
            assertEquals(1, ReliableUdpFrameCodec.decode(ack1.content()).sequence());
            ack1.release();

            DatagramPacket delivered = channel.readInbound();
            byte[] body = new byte[delivered.content().readableBytes()];
            delivered.content().readBytes(body);
            assertArrayEquals(new byte[]{1, 2, 3, 4}, body);
            assertEquals(REMOTE, delivered.sender());
            delivered.release();
            assertEquals(1, handler.snapshot().deliveredDatagrams());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void peerWindowsAreIsolatedAndGenerationChangeResetsInboundState() {
        var handler = new ReliableUdpChannelHandler(limits(8, 8), 77, System::nanoTime);
        var channel = new EmbeddedChannel(handler);
        var second = new InetSocketAddress("127.0.0.1", 9002);
        try {
            assertTrue(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(10, 1, 1, 0, 1, new byte[]{1})));
            assertTrue(writeInbound(channel, second,
                    ReliableUdpFrame.data(20, 1, 1, 0, 1, new byte[]{2})));
            assertTrue(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(11, 1, 1, 0, 1, new byte[]{3})));

            for (int index = 0; index < 6; index++) {
                ((DatagramPacket) channel.readOutbound()).release();
            }
            for (int index = 0; index < 3; index++) {
                ((DatagramPacket) channel.readInbound()).release();
            }
            assertEquals(2, handler.snapshot().peers());
            assertEquals(1, handler.snapshot().generationResets());
            assertEquals(3, handler.snapshot().deliveredDatagrams());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void reconnectStartsNewWindowRejectsRetiredGenerationAndReportsAbandonedFrames() {
        var reconnects = new ArrayList<ReliableUdpChannelHandler.PeerReconnected>();
        var handler = new ReliableUdpChannelHandler(limits(8, 8), 77, System::nanoTime);
        var channel = new EmbeddedChannel(handler, new ChannelInboundHandlerAdapter() {
            @Override
            public void userEventTriggered(io.netty.channel.ChannelHandlerContext ctx, Object event) throws Exception {
                if (event instanceof ReliableUdpChannelHandler.PeerReconnected reconnect) reconnects.add(reconnect);
                ctx.fireUserEventTriggered(event);
            }
        });
        try {
            assertTrue(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(10, 1, 1, 0, 1, new byte[]{1})));
            ((DatagramPacket) channel.readOutbound()).release();
            ((DatagramPacket) channel.readOutbound()).release();
            ((DatagramPacket) channel.readInbound()).release();

            assertTrue(channel.writeOutbound(new DatagramPacket(
                    Unpooled.wrappedBuffer(new byte[]{8}), REMOTE)));
            ((DatagramPacket) channel.readOutbound()).release();
            
            assertTrue(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(11, 1, 2, 0, 1, new byte[]{2})));
            ((DatagramPacket) channel.readOutbound()).release();
            ((DatagramPacket) channel.readOutbound()).release();
            ((DatagramPacket) channel.readInbound()).release();

            assertEquals(1, reconnects.size());
            assertEquals(10, reconnects.getFirst().previousGeneration());
            assertEquals(11, reconnects.getFirst().currentGeneration());
            assertEquals(1, reconnects.getFirst().abandonedFragments());

            assertFalse(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(10, 1, 3, 0, 1, new byte[]{3})));
            assertNull(channel.readOutbound());
            assertNull(channel.readInbound());
            assertEquals(1, handler.snapshot().generationResets());
            assertTrue(handler.snapshot().rejectedFrames() >= 1);
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void freshHelloCreatesPeerBeforeFirstDataFrame() {
        var handler = new ReliableUdpChannelHandler(limits(8, 8), 77, System::nanoTime);
        var channel = new EmbeddedChannel(handler);
        try {
            assertFalse(writeInbound(channel, REMOTE, ReliableUdpFrame.hello(90)));
            assertNull(channel.readOutbound());

            assertTrue(writeInbound(channel, REMOTE,
                    ReliableUdpFrame.data(90, 1, 1, 0, 1, new byte[]{4, 5})));
            DatagramPacket ack = channel.readOutbound();
            assertEquals(ReliableUdpFrame.Kind.ACK, ReliableUdpFrameCodec.decode(ack.content()).kind());
            ack.release();
            DatagramPacket hello = channel.readOutbound();
            assertEquals(ReliableUdpFrame.Kind.HELLO, ReliableUdpFrameCodec.decode(hello.content()).kind());
            hello.release();

            DatagramPacket delivered = channel.readInbound();
            assertNotNull(delivered);
            byte[] body = new byte[delivered.content().readableBytes()];
            delivered.content().readBytes(body);
            assertArrayEquals(new byte[]{4, 5}, body);
            delivered.release();
            assertEquals(1, handler.snapshot().peers());
            assertEquals(1, handler.snapshot().deliveredDatagrams());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
    @Test
    void corruptedFrameIsDroppedWithoutClosingSharedUdpChannel() {
        var handler = new ReliableUdpChannelHandler(limits(8, 8), 77, System::nanoTime);
        var channel = new EmbeddedChannel(handler);
        try {
            var wire = ReliableUdpFrameCodec.encode(channel.alloc(),
                    ReliableUdpFrame.data(10, 1, 1, 0, 1, new byte[]{1}));
            wire.setByte(10, wire.getByte(10) ^ 1);
            assertFalse(channel.writeInbound(new DatagramPacket(wire, LOCAL, REMOTE)));
            assertTrue(channel.isActive());
            assertEquals(1, handler.snapshot().malformedFrames());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void maintenanceRetransmitsUnacknowledgedFrame() {
        var clock = new AtomicLong();
        var handler = new ReliableUdpChannelHandler(limits(8, 8), 77, clock::get);
        var channel = new EmbeddedChannel(handler);
        try {
            assertTrue(channel.writeOutbound(new DatagramPacket(
                    Unpooled.wrappedBuffer(new byte[]{1}), REMOTE)));
            ((DatagramPacket) channel.readOutbound()).release();
            DatagramPacket first = channel.readOutbound();
            var firstFrame = ReliableUdpFrameCodec.decode(first.content());
            first.release();

            clock.set(Duration.ofMillis(50).toNanos());
            channel.advanceTimeBy(10, java.util.concurrent.TimeUnit.MILLISECONDS);
            channel.runScheduledPendingTasks();

            DatagramPacket retry = channel.readOutbound();
            assertNotNull(retry);
            var retryFrame = ReliableUdpFrameCodec.decode(retry.content());
            assertEquals(firstFrame.sequence(), retryFrame.sequence());
            assertArrayEquals(firstFrame.payload(), retryFrame.payload());
            retry.release();
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    private static boolean writeInbound(EmbeddedChannel channel, InetSocketAddress sender,
                                        ReliableUdpFrame frame) {
        var wire = ReliableUdpFrameCodec.encode(channel.alloc(), frame);
        return channel.writeInbound(new DatagramPacket(wire, LOCAL, sender));
    }
}

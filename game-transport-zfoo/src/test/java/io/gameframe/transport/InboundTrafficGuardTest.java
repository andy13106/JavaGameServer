package io.gameframe.transport;

import com.zfoo.net.packet.DecodedPacketInfo;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class InboundTrafficGuardTest {
    @Test
    void byteBudgetResetsPerWindow() {
        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
            10, 5, Duration.ofSeconds(1), Duration.ofSeconds(10)));
        var channel = new EmbeddedChannel();
        try {
            assertTrue(guard.admitBytes(channel, 4, 100));
            assertFalse(guard.admitBytes(channel, 2, 101));
            assertEquals(1, guard.snapshot(channel).byteRateRejections());
            assertTrue(guard.admitBytes(channel, 2, Duration.ofSeconds(1).toNanos() + 101));
            assertEquals(6, guard.snapshot(channel).acceptedBytes());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void rawByteHandlerClosesOnOverflow() {
        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
            10, 4, Duration.ofSeconds(1), Duration.ofSeconds(10)));
        var channel = new EmbeddedChannel(new InboundByteLimitHandler(guard));
        try {
            assertTrue(channel.writeInbound(Unpooled.buffer(4).writeZero(4)));
            assertNotNull(channel.readInbound());
            assertFalse(channel.writeInbound(Unpooled.buffer(1).writeByte(1)));
            assertFalse(channel.isActive());
            assertEquals(1, guard.snapshot(channel).byteRateRejections());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void decodedPacketHandlerCountsAndClosesOnPacketOverflow() {
        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
            2, 100, Duration.ofSeconds(1), Duration.ofSeconds(10)));
        var channel = new EmbeddedChannel(new InboundPacketLimitHandler(guard));
        try {
            assertTrue(channel.writeInbound(DecodedPacketInfo.valueOf(new Object(), null)));
            assertTrue(channel.writeInbound(DecodedPacketInfo.valueOf(new Object(), null)));
            assertNotNull(channel.readInbound());
            assertNotNull(channel.readInbound());
            assertFalse(channel.writeInbound(DecodedPacketInfo.valueOf(new Object(), null)));
            assertFalse(channel.isActive());
            assertEquals(1, guard.snapshot(channel).packetRateRejections());
        } finally {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void decodedActivityTimeoutClosesIdleChannel() {
        var clock = new AtomicLong();
        var guard = new InboundTrafficGuard(new InboundTrafficGuard.Limits(
            10, 100, Duration.ofSeconds(1), Duration.ofMillis(20)), clock::get);
        var channel = new EmbeddedChannel(new InboundPacketLimitHandler(guard));
        try {
            clock.set(Duration.ofMillis(21).toNanos());
            channel.advanceTimeBy(21, TimeUnit.MILLISECONDS);
            channel.runScheduledPendingTasks();
            assertFalse(channel.isActive());
            assertEquals(1, guard.snapshot(channel).heartbeatTimeouts());
        } finally {
            channel.finishAndReleaseAll();
        }
    }
}


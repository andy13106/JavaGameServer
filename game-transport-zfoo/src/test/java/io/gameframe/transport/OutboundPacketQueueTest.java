package io.gameframe.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutboundPacketQueueTest {

    @Test
    void latestStateOverwritesQueuedPacketWithSameKey() {
        var queue = new OutboundPacketQueue(new OutboundPacketQueue.Limits(4, 64, 128));
        var channel = new EmbeddedChannel();

        var first = queue.sendLatest(channel, "player:7", packet(1), 4);
        var second = queue.sendLatest(channel, "player:7", packet(2), 4);
        channel.runPendingTasks();

        assertEquals(OutboundPacketQueue.SendOutcome.SUPERSEDED, first.toCompletableFuture().join());
        assertEquals(OutboundPacketQueue.SendOutcome.SENT, second.toCompletableFuture().join());
        ByteBuf written = channel.readOutbound();
        assertEquals(2, written.readInt());
        written.release();

        var snapshot = queue.snapshot(channel);
        assertEquals(2, snapshot.accepted());
        assertEquals(1, snapshot.coalesced());
        assertEquals(1, snapshot.sent());
        assertEquals(0, snapshot.pendingBytes());
        channel.finishAndReleaseAll();
    }

    @Test
    void persistentPacketsAreNeverCoalesced() {
        var queue = new OutboundPacketQueue(new OutboundPacketQueue.Limits(4, 64, 128));
        var channel = new EmbeddedChannel();

        var first = queue.send(channel, packet(11), 4);
        var second = queue.send(channel, packet(12), 4);
        channel.runPendingTasks();

        assertEquals(OutboundPacketQueue.SendOutcome.SENT, first.toCompletableFuture().join());
        assertEquals(OutboundPacketQueue.SendOutcome.SENT, second.toCompletableFuture().join());
        ByteBuf firstWritten = channel.readOutbound();
        ByteBuf secondWritten = channel.readOutbound();
        assertEquals(11, firstWritten.readInt());
        assertEquals(12, secondWritten.readInt());
        firstWritten.release();
        secondWritten.release();
        assertEquals(0, queue.snapshot(channel).coalesced());
        channel.finishAndReleaseAll();
    }

    @Test
    void packetBudgetRejectsWithoutDroppingAcceptedPacket() {
        var queue = new OutboundPacketQueue(new OutboundPacketQueue.Limits(1, 64, 128));
        var channel = new EmbeddedChannel();

        var accepted = queue.send(channel, packet(21), 4);
        var rejected = queue.send(channel, packet(22), 4);
        channel.runPendingTasks();

        assertEquals(OutboundPacketQueue.SendOutcome.SENT, accepted.toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> rejected.toCompletableFuture().join());
        assertEquals(1, queue.snapshot(channel).rejectedPackets());
        ByteBuf written = channel.readOutbound();
        assertEquals(21, written.readInt());
        written.release();
        channel.finishAndReleaseAll();
    }

    @Test
    void oversizedReplacementIsRejectedAndOriginalRemains() {
        var queue = new OutboundPacketQueue(new OutboundPacketQueue.Limits(2, 8, 16));
        var channel = new EmbeddedChannel();

        var original = queue.sendLatest(channel, "entity:3", packet(31), 4);
        var rejected = queue.sendLatest(channel, "entity:3", Unpooled.buffer(9).writeZero(9), 9);
        channel.runPendingTasks();

        assertEquals(OutboundPacketQueue.SendOutcome.SENT, original.toCompletableFuture().join());
        assertThrows(CompletionException.class, () -> rejected.toCompletableFuture().join());
        assertEquals(1, queue.snapshot(channel).rejectedBytes());
        assertTrue(channel.isActive());
        ByteBuf written = channel.readOutbound();
        assertEquals(31, written.readInt());
        written.release();
        channel.finishAndReleaseAll();
    }

    private static ByteBuf packet(int value) {
        return Unpooled.buffer(4).writeInt(value);
    }
}

package io.gameframe.transport;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ReliableUdpFragmentsTest {
    private static ReliableUdpFragments.Reassembler reassembler(long maxBytes) {
        return new ReliableUdpFragments.Reassembler(new ReliableUdpFragments.Reassembler.Limits(
                2, maxBytes, Duration.ofNanos(10)));
    }

    @Test
    void splitsAndReassemblesOutOfOrderFragments() {
        byte[] payload = new byte[]{0, 1, 2, 3, 4};
        var parts = ReliableUdpFragments.split(payload, 2);
        assertEquals(3, parts.size());
        var reassembler = reassembler(10);

        assertEquals(ReliableUdpFragments.Reassembler.Status.BUFFERED,
                reassembler.accept(ReliableUdpFrame.data(1, 2, 7, 1, 3, parts.get(1)), 0).status());
        assertEquals(ReliableUdpFragments.Reassembler.Status.BUFFERED,
                reassembler.accept(ReliableUdpFrame.data(1, 1, 7, 0, 3, parts.get(0)), 0).status());
        var result = reassembler.accept(ReliableUdpFrame.data(1, 3, 7, 2, 3, parts.get(2)), 0);
        assertEquals(ReliableUdpFragments.Reassembler.Status.COMPLETE, result.status());
        assertArrayEquals(payload, result.payload());
        assertEquals(0, reassembler.bufferedBytes());
    }

    @Test
    void duplicateFragmentDoesNotConsumeMoreBudget() {
        var reassembler = reassembler(4);
        var frame = ReliableUdpFrame.data(1, 1, 2, 0, 2, new byte[]{1, 2});
        assertEquals(ReliableUdpFragments.Reassembler.Status.BUFFERED, reassembler.accept(frame, 0).status());
        assertEquals(ReliableUdpFragments.Reassembler.Status.DUPLICATE, reassembler.accept(frame, 1).status());
        assertEquals(2, reassembler.bufferedBytes());
    }

    @Test
    void byteLimitRejectsAndReleasesWholeAssembly() {
        var reassembler = reassembler(3);
        reassembler.accept(ReliableUdpFrame.data(1, 1, 2, 0, 2, new byte[]{1, 2}), 0);
        var result = reassembler.accept(ReliableUdpFrame.data(1, 2, 2, 1, 2, new byte[]{3, 4}), 0);
        assertEquals(ReliableUdpFragments.Reassembler.Status.REJECTED, result.status());
        assertEquals(0, reassembler.pendingMessages());
        assertEquals(0, reassembler.bufferedBytes());
    }

    @Test
    void incompleteAssemblyExpires() {
        var reassembler = reassembler(4);
        reassembler.accept(ReliableUdpFrame.data(1, 1, 2, 0, 2, new byte[]{1}), 0);
        assertEquals(0, reassembler.expire(9));
        assertEquals(1, reassembler.expire(10));
        assertEquals(0, reassembler.pendingMessages());
    }
}

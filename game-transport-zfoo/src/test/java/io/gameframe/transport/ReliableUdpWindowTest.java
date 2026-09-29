package io.gameframe.transport;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class ReliableUdpWindowTest {
    private static ReliableUdpWindow<String> window() {
        return new ReliableUdpWindow<>(new ReliableUdpWindow.Limits(2, 10, Duration.ofNanos(10), 3, 2));
    }

    @Test
    void ackReleasesPendingBudget() {
        var window = window();
        assertEquals(1, window.offer("a", 6, 0).sequence());
        assertNull(window.offer("b", 5, 0));
        assertTrue(window.acknowledge(1));
        assertEquals(2, window.offer("b", 5, 0).sequence());
        assertEquals(1, window.snapshot().acknowledged());
    }

    @Test
    void dueRetransmitsThenExpiresAtAttemptLimit() {
        var window = window();
        window.offer("a", 2, 0);
        assertEquals(List.of(), window.due(9));
        assertEquals(2, window.due(10).getFirst().attempts());
        assertEquals(3, window.due(20).getFirst().attempts());
        assertEquals(List.of(), window.due(30));
        assertEquals(1, window.snapshot().expired());
    }

    @Test
    void outOfOrderPacketsAreBufferedAndThenDrained() {
        var window = window();
        assertEquals(ReliableUdpWindow.Kind.BUFFERED, window.receive(2, "b").kind());
        var delivered = window.receive(1, "a");
        assertEquals(ReliableUdpWindow.Kind.DELIVERED, delivered.kind());
        assertEquals(List.of("a", "b"), delivered.deliveries().stream()
                .map(ReliableUdpWindow.Sequenced::payload).toList());
    }

    @Test
    void duplicatesAndFarAheadPacketsAreRejected() {
        var window = window();
        window.receive(1, "a");
        assertEquals(ReliableUdpWindow.Kind.DUPLICATE, window.receive(1, "a2").kind());
        assertEquals(ReliableUdpWindow.Kind.REJECTED, window.receive(5, "d").kind());
        assertEquals(1, window.snapshot().duplicates());
        assertEquals(1, window.snapshot().rejected());
    }

    @Test
    void sequenceWrapsAcrossUnsignedIntBoundary() {
        var window = new ReliableUdpWindow<String>(
                new ReliableUdpWindow.Limits(3, 10, Duration.ofNanos(10), 3, 2),
                ReliableUdpWindow.MAX_SEQUENCE);
        assertEquals(ReliableUdpWindow.MAX_SEQUENCE, window.offer("last", 1, 0).sequence());
        assertEquals(0, window.offer("zero", 1, 0).sequence());

        assertEquals(ReliableUdpWindow.Kind.BUFFERED, window.receive(0, "zero").kind());
        var delivered = window.receive(ReliableUdpWindow.MAX_SEQUENCE, "last");
        assertEquals(List.of("last", "zero"), delivered.deliveries().stream()
                .map(ReliableUdpWindow.Sequenced::payload).toList());
        assertEquals(1, window.snapshot().nextExpected());
    }
}

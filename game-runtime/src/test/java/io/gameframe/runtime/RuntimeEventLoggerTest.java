package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RuntimeEventLoggerTest {
    @Test
    void emitsBoundedJsonAndRedactsSensitiveFields() {
        var events = new ArrayList<RuntimeEventLogger.Event>();
        var logger = new RuntimeEventLogger(events::add);
        logger.info("service.node.active", Map.of("instance", "game-a", "token", "never-log", "message", "line\nnext"));
        assertEquals(1, events.size());
        var event = events.getFirst();
        assertEquals("[REDACTED]", event.fields().get("token"));
        assertTrue(event.json().contains("\\n"));
        assertFalse(event.json().contains("never-log"));
    }

    @Test
    void sinkFailureAndBoundsDoNotLeakIntoRuntime() {
        var logger = new RuntimeEventLogger(ignored -> { throw new AssertionError("sink failed"); }, 1, 8);
        assertDoesNotThrow(() -> logger.warn("service.node.failed", Map.of("reason", "failure")));
        assertThrows(IllegalArgumentException.class,
                () -> logger.info("service.node.active", Map.of("one", "1", "two", "2")));
        assertThrows(IllegalArgumentException.class,
                () -> logger.info("service.node.active", Map.of("reason", "too-long-value")));
        assertThrows(IllegalArgumentException.class,
                () -> logger.info("service.node.active", Map.of("bad field", "x")));
    }
}
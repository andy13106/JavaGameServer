package io.gameframe.demo;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class ZoneDemoTest {
    @Test void configuredPlayerTransfersWithEventsAndStopUnsubscribes() throws Exception {
        var result = ZoneDemo.run();
        assertEquals("ACCEPTED", result.outcome()); assertEquals(0, result.sourceObjects());
        assertEquals(1, result.targetObjects()); assertEquals(108, result.gold());
        assertEquals(0, result.subscriptionsAfterStop());
    }
}

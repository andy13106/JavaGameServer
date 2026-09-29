package io.gameframe.demo;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class PersistenceDemoTest {
    @Test void componentsCoalescePeriodicallyAndDrainBeforeActorStops() throws Exception {
        assertEquals(new PersistenceDemo.Summary(100, 2, 6, 4, 2, 203, 4, 0), PersistenceDemo.run());
    }
}

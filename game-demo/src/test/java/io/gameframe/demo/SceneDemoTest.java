package io.gameframe.demo;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SceneDemoTest {
    @Test void componentMovementVisibilityAndBatchingWorkTogether() throws Exception {
        var summary = SceneDemo.run();
        assertEquals(4, summary.movementTicks());
        assertEquals(0, summary.passiveTicks());
        assertEquals(10, summary.visibilityChanges());
        assertEquals(1, summary.firstBatchEncoded());
        assertEquals(1, summary.firstBatchReused());
        assertEquals(3, summary.acceptedPackets());
        assertEquals(2, summary.mergedStates());
        assertEquals(8.0, summary.observer2Position());
        assertEquals(6.0, summary.observer3Position());
    }
}

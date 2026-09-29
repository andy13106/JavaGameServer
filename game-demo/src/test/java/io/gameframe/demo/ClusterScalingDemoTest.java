package io.gameframe.demo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClusterScalingDemoTest {
    @Test
    void scalesOutRecoversGenerationRoutesDrainsAndScalesIn() throws Exception {
        var summary = ClusterScalingDemo.run();
        assertEquals("game-a", summary.beforeDrain());
        assertEquals("game-a", summary.afterRecovery());
        assertEquals("game-b", summary.afterDrain());
        assertEquals(2, summary.registered());
        assertEquals(1, summary.remaining());
        assertEquals(1, summary.removed());
        assertTrue(summary.heartbeatFailureDetected());
        assertTrue(summary.reRegistered());
        assertEquals(2, summary.recoveredGeneration());
    }
}
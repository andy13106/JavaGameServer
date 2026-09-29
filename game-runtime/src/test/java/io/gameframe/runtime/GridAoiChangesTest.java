package io.gameframe.runtime;

import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class GridAoiChangesTest {
    @Test void enterCrossCellMoveAndLeaveEmitSymmetricChanges() {
        var grid = new GridAoi(10, 1);
        assertTrue(grid.moveWithChanges(1, -1, 0).isEmpty());
        assertEquals(Set.of(
            new GridAoi.VisibilityChange(2, 1, true), new GridAoi.VisibilityChange(1, 2, true)),
            Set.copyOf(grid.moveWithChanges(2, 0, 0)));
        assertTrue(grid.moveWithChanges(2, 9, 9).isEmpty());
        grid.moveWithChanges(3, 40, 0);
        assertEquals(Set.of(
            new GridAoi.VisibilityChange(2, 1, false), new GridAoi.VisibilityChange(1, 2, false),
            new GridAoi.VisibilityChange(2, 3, true), new GridAoi.VisibilityChange(3, 2, true)),
            Set.copyOf(grid.moveWithChanges(2, 40, 0)));
        assertEquals(2, grid.removeWithChanges(3).size());
        assertTrue(grid.removeWithChanges(3).isEmpty());
        assertTrue(grid.visible(2).isEmpty());
    }
    @Test void invalidMoveAndCapacityRejectionPreserveTheOldIndex() throws Exception {
        try (var system = new ActorSystem(1, 1, 16, 4)) {
            var actor = system.spawn("map");
            var grid = new GridAoi(actor, 10, 1, 2);
            assertThrows(IllegalStateException.class, grid::size);
            actor.tell(() -> {
                grid.move(1, 0, 0); grid.move(2, 1, 1);
                assertThrows(IllegalArgumentException.class, () -> grid.moveWithChanges(1, Double.NaN, 0));
                assertThrows(RejectedExecutionException.class, () -> grid.moveWithChanges(3, 0, 0));
                assertEquals(2, grid.size());
                assertEquals(Set.of(2L), grid.visible(1));
                assertFalse(grid.contains(3));
            }).toCompletableFuture().get(3, TimeUnit.SECONDS);
        }
    }
    @Test void randomMovementMatchesBruteForceVisibilityAndDeltaEvents() {
        var grid = new GridAoi(10, 1);
        var positions = new HashMap<Long, double[]>();
        var random = new Random(72019);
        for (int step = 0; step < 300; step++) {
            long entity = random.nextInt(12) + 1;
            Set<String> before = pairs(positions);
            List<GridAoi.VisibilityChange> actual;
            if (random.nextInt(4) == 0) {
                positions.remove(entity);
                actual = grid.removeWithChanges(entity);
            } else {
                double x = random.nextInt(100) - 50, y = random.nextInt(100) - 50;
                positions.put(entity, new double[] {x, y});
                actual = grid.moveWithChanges(entity, x, y);
            }
            Set<String> after = pairs(positions), expected = new HashSet<>();
            for (String pair : after) if (!before.contains(pair)) expected.add(pair + ":true");
            for (String pair : before) if (!after.contains(pair)) expected.add(pair + ":false");
            Set<String> changes = new HashSet<>();
            for (var change : actual) changes.add(change.observer() + ":" + change.target() + ":" + change.entered());
            assertEquals(expected, changes, "step " + step);
            assertEquals(changes.size(), actual.size(), "no duplicate notifications");
            for (long id : positions.keySet()) {
                var visible = new HashSet<Long>();
                for (long target : positions.keySet()) if (after.contains(id + ":" + target)) visible.add(target);
                assertEquals(visible, grid.visible(id));
            }
        }
    }
    private static Set<String> pairs(Map<Long, double[]> positions) {
        var result = new HashSet<String>();
        for (var observer : positions.entrySet()) for (var target : positions.entrySet()) {
            if (observer.getKey().equals(target.getKey())) continue;
            double[] a = observer.getValue(), b = target.getValue();
            if (Math.abs(Math.floor(a[0] / 10) - Math.floor(b[0] / 10)) <= 1
                && Math.abs(Math.floor(a[1] / 10) - Math.floor(b[1] / 10)) <= 1)
                result.add(observer.getKey() + ":" + target.getKey());
        }
        return result;
    }
}

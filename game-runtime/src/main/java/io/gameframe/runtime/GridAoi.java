package io.gameframe.runtime;

import java.util.*;
import java.util.concurrent.RejectedExecutionException;

/** Cell-radius visibility, not geometric distance. Use the Actor constructor for checked ownership. */
public final class GridAoi {
    public record VisibilityChange(long observer, long target, boolean entered) {}
    private record Cell(int x, int y) {}
    private final double cellSize;
    private final int radius, maxEntities;
    private final Runnable requireCurrent;
    private final Map<Long, Cell> positions = new HashMap<>();
    private final Map<Cell, Set<Long>> cells = new HashMap<>();

    /** Compatibility constructor: caller enforces Actor ownership and entity capacity. */
    public GridAoi(double cellSize, int radius) {
        this(cellSize, radius, Integer.MAX_VALUE, () -> {});
    }
    public GridAoi(ActorSystem.ActorRef actor, double cellSize, int radius, int maxEntities) {
        this(cellSize, radius, maxEntities, Objects.requireNonNull(actor)::requireCurrent);
    }
    private GridAoi(double cellSize, int radius, int maxEntities, Runnable requireCurrent) {
        if (!Double.isFinite(cellSize) || cellSize <= 0 || radius < 0 || radius > 64 || maxEntities < 1)
            throw new IllegalArgumentException("invalid AOI limits");
        this.cellSize = cellSize;
        this.radius = radius;
        this.maxEntities = maxEntities;
        this.requireCurrent = requireCurrent;
    }
    public int size() { requireCurrent.run(); return positions.size(); }
    public boolean contains(long entityId) { requireCurrent.run(); return positions.containsKey(entityId); }

    /** Upsert retained for compatibility; use moveWithChanges when notifications are needed. */
    public void move(long entityId, double x, double y) {
        requireCurrent.run();
        moveTo(entityId, toCell(x, y));
    }
    /** Returns committed, immutable changes; notification callbacks execute outside index mutation. */
    public List<VisibilityChange> moveWithChanges(long entityId, double x, double y) {
        requireCurrent.run();
        Cell next = toCell(x, y); // Validate before touching the old position.
        if (next.equals(positions.get(entityId))) return List.of();
        Set<Long> before = visible(entityId);
        moveTo(entityId, next);
        return changes(entityId, before, visible(entityId));
    }
    public void remove(long entityId) {
        requireCurrent.run();
        Cell cell = positions.remove(entityId);
        if (cell == null) return;
        removeFromCell(entityId, cell);
    }
    public List<VisibilityChange> removeWithChanges(long entityId) {
        requireCurrent.run();
        Set<Long> before = visible(entityId);
        remove(entityId);
        return changes(entityId, before, Set.of());
    }
    private void moveTo(long entityId, Cell next) {
        Cell previous = positions.get(entityId);
        if (next.equals(previous)) return;
        if (previous == null && positions.size() >= maxEntities)
            throw new RejectedExecutionException("AOI entity capacity");
        if (previous != null) removeFromCell(entityId, previous);
        positions.put(entityId, next);
        cells.computeIfAbsent(next, ignored -> new HashSet<>()).add(entityId);
    }
    private void removeFromCell(long entityId, Cell cell) {
        var members = cells.get(cell);
        members.remove(entityId);
        if (members.isEmpty()) cells.remove(cell);
    }
    private Cell toCell(double x, double y) {
        if (!Double.isFinite(x) || !Double.isFinite(y)
            || Math.abs(x / cellSize) > 1_000_000 || Math.abs(y / cellSize) > 1_000_000)
            throw new IllegalArgumentException("coordinate out of range");
        return new Cell((int) Math.floor(x / cellSize), (int) Math.floor(y / cellSize));
    }
    public Set<Long> visible(long entityId) {
        requireCurrent.run();
        Cell center = positions.get(entityId);
        if (center == null) return Set.of();
        Set<Long> result = new HashSet<>();
        for (int x = center.x - radius; x <= center.x + radius; x++)
            for (int y = center.y - radius; y <= center.y + radius; y++)
                result.addAll(cells.getOrDefault(new Cell(x, y), Set.of()));
        result.remove(entityId);
        return Set.copyOf(result);
    }
    private static List<VisibilityChange> changes(long entity, Set<Long> before, Set<Long> after) {
        var result = new ArrayList<VisibilityChange>();
        var left = new TreeSet<>(before); left.removeAll(after);
        var entered = new TreeSet<>(after); entered.removeAll(before);
        for (long other : left) {
            result.add(new VisibilityChange(entity, other, false));
            result.add(new VisibilityChange(other, entity, false));
        }
        for (long other : entered) {
            result.add(new VisibilityChange(entity, other, true));
            result.add(new VisibilityChange(other, entity, true));
        }
        return List.copyOf(result);
    }
}

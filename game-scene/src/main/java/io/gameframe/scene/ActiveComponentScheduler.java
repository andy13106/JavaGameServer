package io.gameframe.scene;

import io.gameframe.runtime.ActorSystem;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;

/** One per room/map Actor. Only active components enter the bounded round-robin set. */
public final class ActiveComponentScheduler {
    public record Failure(long entityId, int typeId, RuntimeException cause) {}
    public record Stats(int active, int examined, int executed, int deferred, List<Failure> failures) {
        public Stats { failures = List.copyOf(failures); }
    }
    private static final class Entry {
        final GameComponent component;
        final UpdatePolicy policy;
        final long entityId;
        long nanos, ticks;
        Entry(GameComponent component, UpdatePolicy policy) {
            this.component = component;
            this.policy = policy;
            this.entityId = component.owner().id();
        }
        boolean due() {
            return switch (policy.mode()) {
                case PASSIVE -> false;
                case EVERY_FRAME -> true;
                case EVERY_N_TICKS -> ticks >= policy.interval();
                case INTERVAL -> nanos >= policy.interval();
            };
        }
    }
    private final ActorSystem.ActorRef actor;
    private final int maxActive, maxUpdates;
    private final LinkedHashMap<GameComponent, Entry> active = new LinkedHashMap<>();
    private boolean updating;

    public ActiveComponentScheduler(ActorSystem.ActorRef actor, int maxActive, int maxUpdates) {
        this.actor = Objects.requireNonNull(actor);
        if (maxActive < 1 || maxUpdates < 1) throw new IllegalArgumentException("positive budgets required");
        this.maxActive = maxActive;
        this.maxUpdates = maxUpdates;
    }
    ActorSystem.ActorRef actor() { return actor; }
    public int activeCount() { actor.requireCurrent(); return active.size(); }
    void configure(GameComponent component, UpdatePolicy policy) {
        actor.requireCurrent();
        if (!policy.active()) { active.remove(component); return; }
        if (!active.containsKey(component) && active.size() >= maxActive)
            throw new RejectedExecutionException("active component capacity");
        active.put(component, new Entry(component, policy));
    }
    void unregister(GameComponent component) { actor.requireCurrent(); active.remove(component); }

    /** delta is simulation time; one callback per component per call, never an unbounded catch-up loop. */
    public Stats update(long deltaNanos) {
        actor.requireCurrent();
        if (deltaNanos <= 0) throw new IllegalArgumentException("positive delta required");
        if (updating) throw new IllegalStateException("recursive scheduler update");
        updating = true;
        try {
            var batch = List.copyOf(active.values());
            for (var entry : batch) {
                entry.nanos = saturatedAdd(entry.nanos, deltaNanos);
                entry.ticks = saturatedAdd(entry.ticks, 1);
            }
            int executed = 0, deferred = 0;
            var failures = new ArrayList<Failure>();
            for (var entry : batch) {
                GameComponent component = entry.component;
                if (active.get(component) != entry || !entry.due()) continue;
                if (executed >= maxUpdates) { deferred++; continue; }
                long elapsed = entry.nanos;
                entry.nanos = 0; entry.ticks = 0;
                active.remove(component);
                active.put(component, entry); // Give deferred entries first chance on the next tick.
                executed++;
                try { component.onUpdate(elapsed); }
                catch (RuntimeException error) {
                    // Failed logic must not silently continue ticking partially updated state.
                    if (active.containsKey(component)) component.updatePolicy(UpdatePolicy.passive());
                    failures.add(new Failure(entry.entityId, component.typeId(), error));
                }
            }
            return new Stats(active.size(), batch.size(), executed, deferred, failures);
        } finally { updating = false; }
    }
    private static long saturatedAdd(long value, long delta) {
        return value > Long.MAX_VALUE - delta ? Long.MAX_VALUE : value + delta;
    }
}

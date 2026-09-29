package io.gameframe.scene;

import io.gameframe.runtime.ActorSystem;
import io.gameframe.runtime.GridAoi;
import java.util.*;

/**
 * Actor-owned, bounded latest-state batching. Payloads must be absolute, observer-independent
 * public movement state. This is a lossy refresh stream, not a reliable incremental-delta channel.
 */
public final class MovementReplicator {
    public record Limits(int maxDirtyEntities, long maxPendingBytes, int maxStateBytes,
                         int maxPacketBytes, int maxVisibilityLinks) {
        public Limits {
            if (maxDirtyEntities < 1 || maxPendingBytes < 1 || maxStateBytes < 1
                || maxPacketBytes < 1 || maxVisibilityLinks < 1 || maxStateBytes > maxPendingBytes)
                throw new IllegalArgumentException("invalid movement limits");
        }
    }
    public record State(long entityId, byte[] payload) {
        public State { payload = Objects.requireNonNull(payload).clone(); }
        @Override public byte[] payload() { return payload.clone(); }
        public int byteSize() { return payload.length; }
    }
    /** No observer parameter: shared encoding must never embed observer-specific private data. */
    @FunctionalInterface public interface Encoder { byte[] encode(List<State> states); }
    /** true means accepted by the sender, not acknowledged by a remote client. */
    @FunctionalInterface public interface Sender { boolean send(long observer, byte[] packet); }
    public enum MarkResult { ACCEPTED, REPLACED, UNKNOWN_ENTITY, CAPACITY, TOO_LARGE }
    public record PendingStats(int entities, long bytes, long merged, long rejected) {}
    public record Failure(long observer, String stage, RuntimeException cause) {}
    public record FlushStats(int dirtyEntities, int observers, int encodedPackets, int reusedPackets,
                             int acceptedPackets, int rejectedPackets, boolean visibilityBudgetExceeded,
                             List<Failure> failures) {
        public FlushStats { failures = List.copyOf(failures); }
    }
    private final ActorSystem.ActorRef actor;
    private final GridAoi aoi;
    private final Limits limits;
    private final Encoder encoder;
    private final Sender sender;
    private final Map<Long, State> dirty = new HashMap<>();
    private long bytes, merged, rejected;
    private boolean flushing;

    public MovementReplicator(ActorSystem.ActorRef actor, GridAoi aoi, Limits limits, Encoder encoder, Sender sender) {
        this.actor = Objects.requireNonNull(actor);
        this.aoi = Objects.requireNonNull(aoi);
        this.limits = Objects.requireNonNull(limits);
        this.encoder = Objects.requireNonNull(encoder);
        this.sender = Objects.requireNonNull(sender);
    }
    public MarkResult markDirty(long entityId, byte[] payload) {
        actor.requireCurrent();
        Objects.requireNonNull(payload);
        if (!aoi.contains(entityId)) { rejected++; return MarkResult.UNKNOWN_ENTITY; }
        if (payload.length > limits.maxStateBytes()) { rejected++; return MarkResult.TOO_LARGE; }
        State old = dirty.get(entityId);
        long retainedBytes = bytes - (old == null ? 0 : old.byteSize());
        if ((old == null && dirty.size() >= limits.maxDirtyEntities())
            || payload.length > limits.maxPendingBytes() - retainedBytes) {
            rejected++;
            return MarkResult.CAPACITY;
        }
        dirty.put(entityId, new State(entityId, payload));
        bytes = retainedBytes + payload.length;
        if (old != null) { merged++; return MarkResult.REPLACED; }
        return MarkResult.ACCEPTED;
    }
    /** Call after removing the entity from AOI. Prevents pending state surviving a despawn. */
    public void forget(long entityId) {
        actor.requireCurrent();
        State removed = dirty.remove(entityId);
        if (removed != null) bytes -= removed.byteSize();
    }
    public PendingStats pendingStats() { actor.requireCurrent(); return new PendingStats(dirty.size(), bytes, merged, rejected); }

    /**
     * Consumes this refresh batch even on rejection; callers must periodically publish fresh absolute
     * states. Updates marked by callbacks are retained for the next flush. No automatic stale retries.
     */
    public FlushStats flush() {
        actor.requireCurrent();
        if (flushing) throw new IllegalStateException("recursive movement flush");
        flushing = true;
        var batch = Map.copyOf(dirty);
        dirty.clear(); bytes = 0;
        try {
            var byObserver = new TreeMap<Long, List<Long>>();
            int links = 0;
            for (long entity : new TreeSet<>(batch.keySet())) {
                for (long observer : aoi.visible(entity)) {
                    if (++links > limits.maxVisibilityLinks())
                        return new FlushStats(batch.size(), 0, 0, 0, 0, 0, true, List.of());
                    byObserver.computeIfAbsent(observer, ignored -> new ArrayList<>()).add(entity);
                }
            }
            var cache = new HashMap<List<Long>, byte[]>();
            long cachedBytes = 0;
            int encoded = 0, reused = 0, accepted = 0, declined = 0;
            var failures = new ArrayList<Failure>();
            for (var entry : byObserver.entrySet()) {
                long observer = entry.getKey();
                Set<Long> visibleNow = aoi.visible(observer);
                List<Long> key = entry.getValue().stream().filter(visibleNow::contains).toList();
                if (key.isEmpty()) continue;
                byte[] packet = cache.get(key);
                if (packet == null) {
                    try {
                        encoded++;
                        packet = Objects.requireNonNull(encoder.encode(key.stream().map(batch::get).toList()), "null packet");
                        if (packet.length > limits.maxPacketBytes()) { declined++; continue; }
                        packet = packet.clone();
                        if (packet.length <= limits.maxPendingBytes() - cachedBytes) {
                            cache.put(key, packet);
                            cachedBytes += packet.length;
                        }
                    } catch (RuntimeException error) { failures.add(new Failure(observer, "encode", error)); continue; }
                } else { reused++; }
                try {
                    if (sender.send(observer, packet.clone())) accepted++;
                    else declined++;
                } catch (RuntimeException error) { failures.add(new Failure(observer, "send", error)); }
            }
            return new FlushStats(batch.size(), byObserver.size(), encoded, reused, accepted, declined, false, failures);
        } finally { flushing = false; }
    }
}

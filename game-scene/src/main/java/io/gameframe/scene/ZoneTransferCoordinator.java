package io.gameframe.scene;

import io.gameframe.runtime.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Bounded in-process handoff. Hooks run only on their zone actor, and must be short.
 * A source stays frozen until target cancellation is confirmed or ownership is released.
 * Failures after release starts require application reconciliation; commands are never replayed.
 */
public final class ZoneTransferCoordinator implements AutoCloseable {
    public enum Phase { FREEZING, PREPARING, PREPARED, RELEASING, COMMITTING, ABORTING, RECOVERY_REQUIRED, DONE }
    public enum Outcome { ACCEPTED, REJECTED, TIMEOUT, CLOSED, RECOVERY_REQUIRED }
    public record TransferRequest(long transferId, long entityId, String sourceZone, String targetZone,
                                  byte[] payload, long deadlineNanos) {
        public TransferRequest { payload = Objects.requireNonNull(payload).clone(); }
        @Override public byte[] payload() { return payload.clone(); }
    }
    public record TransferResult(TransferRequest request, Outcome outcome, Throwable error) {
        public boolean accepted() { return outcome == Outcome.ACCEPTED; }
    }
    public interface Zone {
        /** Check ownership and freeze the source, including simulation and client commands. */
        void freeze(TransferRequest request);
        /** Remove source ownership. A throw is treated as potentially having released it. */
        void release(TransferRequest request);
        /** Resume the frozen source, only after target cancellation is confirmed. */
        void resume(TransferRequest request);
        /** Stage an inactive target. Never expose it to gameplay before commit. */
        boolean prepare(TransferRequest request);
        /** Activate the staged target. */
        void commit(TransferRequest request);
        /** Idempotently discard staging, including partially failed preparation. */
        void cancel(TransferRequest request);
    }
    private record Endpoint(ActorSystem.ActorRef actor, Zone zone) {}
    public final class Transfer {
        private final TransferRequest request;
        private final Endpoint source, target;
        private final CompletableFuture<TransferResult> result = new CompletableFuture<>();
        private Phase phase = Phase.FREEZING;
        private SharedTickScheduler.Handle timer;
        private int inFlight;
        private Throwable recoveryError;
        private Transfer(TransferRequest request, Endpoint source, Endpoint target) {
            this.request = request; this.source = source; this.target = target;
        }
        public TransferRequest request() { return request; }
        public CompletionStage<TransferResult> result() { return result.minimalCompletionStage(); }
        public synchronized Phase phase() { return phase; }
        public synchronized Throwable recoveryError() { return recoveryError; }
        public void cancel() { abort(this, Outcome.REJECTED, null); }
    }
    private final Map<String, Endpoint> zones = new HashMap<>();
    private final Map<Long, Transfer> pending = new HashMap<>();
    private final Set<Long> transferringEntities = new HashSet<>();
    private final SharedTickScheduler scheduler;
    private final int maxPending, maxPayloadBytes, maxZones;
    private long nextId;
    private boolean closed;

    public ZoneTransferCoordinator(SharedTickScheduler scheduler, int maxPending) {
        this(scheduler, maxPending, 65_536, 1024);
    }
    public ZoneTransferCoordinator(SharedTickScheduler scheduler, int maxPending, int maxPayloadBytes, int maxZones) {
        if (maxPending < 1 || maxPayloadBytes < 1 || maxZones < 2) throw new IllegalArgumentException();
        this.scheduler = Objects.requireNonNull(scheduler);
        this.maxPending = maxPending; this.maxPayloadBytes = maxPayloadBytes; this.maxZones = maxZones;
    }
    public synchronized boolean registerZone(String id, ActorSystem.ActorRef actor, Zone zone) {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("zone id");
        Objects.requireNonNull(actor); Objects.requireNonNull(zone);
        if (closed || zones.size() >= maxZones) throw new RejectedExecutionException("zone registry unavailable");
        return zones.putIfAbsent(id, new Endpoint(actor, zone)) == null;
    }
    public synchronized boolean unregisterZone(String id) {
        if (pending.values().stream().anyMatch(t -> t.request.sourceZone().equals(id) || t.request.targetZone().equals(id)))
            throw new IllegalStateException("zone has pending/recovery transfers");
        return zones.remove(id) != null;
    }
    public synchronized int pendingCount() { return pending.size(); }
    public synchronized List<Transfer> pendingTransfers() { return List.copyOf(pending.values()); }

    /** Invoke on the source actor after encoding its snapshot; admission failures do not freeze it. */
    public Transfer begin(String sourceId, String targetId, long entityId, byte[] payload, Duration timeout) {
        Objects.requireNonNull(payload); Objects.requireNonNull(timeout);
        long nanos = timeout.toNanos();
        if (entityId < 1 || Objects.equals(sourceId, targetId) || nanos < 1_000_000 || payload.length > maxPayloadBytes)
            throw new IllegalArgumentException("invalid transfer or payload/timeout limit");
        Transfer t;
        synchronized (this) {
            var source = zones.get(sourceId); var target = zones.get(targetId);
            if (source == null || target == null) throw new IllegalArgumentException("unknown zone");
            source.actor.requireCurrent();
            if (closed || pending.size() >= maxPending || transferringEntities.contains(entityId))
                throw new RejectedExecutionException("transfer closed, full, or entity already transferring");
            t = new Transfer(new TransferRequest(++nextId, entityId, sourceId, targetId,
                payload, System.nanoTime() + nanos), source, target);
            pending.put(t.request.transferId(), t); transferringEntities.add(entityId);
        }
        try {
            synchronized (t) { t.inFlight++; }
            try { t.source.zone.freeze(t.request); }
            finally { synchronized (t) { t.inFlight--; } }
            synchronized (t) {
                if (t.phase != Phase.FREEZING) return t;
                t.phase = Phase.PREPARING;
                // Publication is protected against a very early timer callback.
                t.timer = scheduler.schedule(timeout, () -> abort(t, Outcome.TIMEOUT, new TimeoutException("zone handoff deadline")));
            }
            submit(t, t.target, () -> {
                synchronized (t) { if (t.phase != Phase.PREPARING) return; }
                boolean ready = t.target.zone.prepare(t.request);
                if (!ready) { abort(t, Outcome.REJECTED, null); return; }
                confirmPrepared(t);
            }, error -> abort(t, Outcome.REJECTED, error));
        } catch (Throwable error) {
            // A partially executed freeze cannot be assumed reversible.
            recover(t, error);
        }
        return t;
    }
    // Internal ACK; duplicate and late confirmations cannot release the source twice.
    boolean confirmPrepared(Transfer t) {
        t.target.actor.requireCurrent();
        synchronized (t) {
            if (t.phase != Phase.PREPARING) return false;
            t.phase = Phase.PREPARED;
        }
        release(t);
        return true;
    }
    private void release(Transfer t) {
        submit(t, t.source, () -> {
            synchronized (t) {
                if (t.phase != Phase.PREPARED) return;
                if (System.nanoTime() - t.request.deadlineNanos() >= 0) {
                    // Leave PREPARED so abort can safely cancel the target.
                } else {
                    t.phase = Phase.RELEASING;
                }
            }
            if (t.phase() != Phase.RELEASING) { abort(t, Outcome.TIMEOUT, null); return; }
            t.source.zone.release(t.request);
            synchronized (t) { if (t.phase != Phase.RELEASING) return; t.phase = Phase.COMMITTING; }
            submit(t, t.target, () -> {
                synchronized (t) { if (t.phase != Phase.COMMITTING) return; }
                t.target.zone.commit(t.request);
                finish(t, Outcome.ACCEPTED, null, Phase.COMMITTING);
            }, error -> recover(t, error));
        }, error -> {
            if (t.phase() == Phase.PREPARED) abort(t, Outcome.REJECTED, error);
            else recover(t, error);
        });
    }
    private void abort(Transfer t, Outcome outcome, Throwable error) {
        boolean uncertain;
        synchronized (t) {
            if (t.phase == Phase.DONE || t.phase == Phase.RECOVERY_REQUIRED || t.phase == Phase.ABORTING) return;
            uncertain = t.phase == Phase.FREEZING || t.phase == Phase.RELEASING || t.phase == Phase.COMMITTING;
            if (!uncertain) { t.phase = Phase.ABORTING; cancelTimer(t); }
        }
        if (uncertain) { recover(t, error == null ? new IllegalStateException("handoff interrupted during ownership change") : error); return; }
        // Target FIFO ensures cancellation runs after any already executing prepare.
        submit(t, t.target, () -> {
            if (t.phase() != Phase.ABORTING) return;
            t.target.zone.cancel(t.request);
            submit(t, t.source, () -> {
                if (t.phase() != Phase.ABORTING) return;
                t.source.zone.resume(t.request);
                finish(t, outcome, error, Phase.ABORTING);
            }, e -> recover(t, e));
        }, e -> recover(t, e));
    }
    private void submit(Transfer t, Endpoint endpoint, Runnable action, java.util.function.Consumer<Throwable> onError) {
        synchronized (t) { t.inFlight++; }
        endpoint.actor.tell(action).whenComplete((v, error) -> {
            try { if (error != null) onError.accept(unwrap(error)); }
            finally { synchronized (t) { t.inFlight--; } }
        });
    }
    private void recover(Transfer t, Throwable error) {
        synchronized (t) {
            if (t.phase == Phase.DONE || t.phase == Phase.RECOVERY_REQUIRED) return;
            t.phase = Phase.RECOVERY_REQUIRED; t.recoveryError = error; cancelTimer(t);
        }
        // Keep the bounded slot and entity guard until explicit reconciliation.
        t.result.complete(new TransferResult(t.request, Outcome.RECOVERY_REQUIRED, error));
    }
    private void finish(Transfer t, Outcome outcome, Throwable error, Phase expected) {
        synchronized (t) {
            if (t.phase != expected) return;
            t.phase = Phase.DONE; cancelTimer(t);
        }
        remove(t);
        t.result.complete(new TransferResult(t.request, outcome, error));
    }
    private static void cancelTimer(Transfer t) { if (t.timer != null) t.timer.close(); }
    private synchronized void remove(Transfer t) {
        pending.remove(t.request.transferId()); transferringEntities.remove(t.request.entityId());
    }
    /**
     * Administrative acknowledgement AFTER the application has reconciled both zones.
     * This performs no gameplay mutation and must never be used as an automatic retry.
     */
    public void acknowledgeRecovery(Transfer t) {
        synchronized (this) {
            if (pending.get(t.request.transferId()) != t) throw new IllegalArgumentException("unknown transfer");
        }
        synchronized (t) {
            if (t.phase != Phase.RECOVERY_REQUIRED || t.inFlight != 0)
                throw new IllegalStateException("recovery not quiescent");
            t.phase = Phase.DONE;
        }
        remove(t);
    }
    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }
    /** Initiates cancellation; inspect pendingTransfers() for blocked actors or recovery work. */
    @Override public void close() {
        List<Transfer> transfers;
        synchronized (this) { if (closed) return; closed = true; transfers = List.copyOf(pending.values()); }
        transfers.forEach(t -> abort(t, Outcome.CLOSED, null));
    }
}

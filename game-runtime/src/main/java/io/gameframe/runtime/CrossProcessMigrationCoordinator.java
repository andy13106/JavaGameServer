package io.gameframe.runtime;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Bounded orchestration for remote actor migration adapters. An optional
 * {@link MigrationJournal} makes phase transitions and recovery state durable;
 * journal versions fence delayed recovery workers after a process restart.
 */
public final class CrossProcessMigrationCoordinator implements AutoCloseable {
    public enum Phase { FREEZING, PREPARING, PREPARED, RELEASING, COMMITTING,
        ABORTING, RECOVERY_REQUIRED, DONE }
    public enum Outcome { ACCEPTED, REJECTED, TIMEOUT, CLOSED, RECOVERY_REQUIRED }

    public record Request(String migrationId, String actorId, String sourceInstance,
                          String targetInstance, long fencingToken, byte[] snapshot,
                          long deadlineMillis) {
        public Request {
            requireText(migrationId, "migrationId");
            requireText(actorId, "actorId");
            requireText(sourceInstance, "sourceInstance");
            requireText(targetInstance, "targetInstance");
            if (sourceInstance.equals(targetInstance) || fencingToken < 1 || deadlineMillis < 1)
                throw new IllegalArgumentException("invalid migration request");
            snapshot = Objects.requireNonNull(snapshot, "snapshot").clone();
        }
        @Override public byte[] snapshot() { return snapshot.clone(); }
    }

    public record Result(Request request, Outcome outcome, Throwable error) {
        public boolean accepted() { return outcome == Outcome.ACCEPTED; }
    }

    public interface Source {
        CompletionStage<Boolean> freeze(Request request);
        CompletionStage<Void> release(Request request);
        CompletionStage<Void> resume(Request request);
    }

    public interface Target {
        CompletionStage<Boolean> prepare(Request request);
        CompletionStage<Void> commit(Request request);
        CompletionStage<Void> cancel(Request request);
    }

    public final class Migration {
        private final Request request;
        private final Source source;
        private final Target target;
        private final CompletableFuture<Result> result = new CompletableFuture<>();
        private volatile Phase phase = Phase.FREEZING;
        private volatile Throwable recoveryError;
        private ScheduledFuture<?> timeout;
        private int inFlight;
        private long journalVersion;
        private CompletionStage<Void> journalTail = CompletableFuture.completedFuture(null);

        private Migration(Request request, Source source, Target target) {
            this.request = request;
            this.source = source;
            this.target = target;
        }
        public Request request() { return request; }
        public CompletionStage<Result> result() { return result.minimalCompletionStage(); }
        public Phase phase() { return phase; }
        public Throwable recoveryError() { return recoveryError; }
        public void cancel() {
            synchronized (CrossProcessMigrationCoordinator.this) {
                abort(this, Outcome.REJECTED, null);
            }
        }
    }

    private final ScheduledExecutorService scheduler;
    private final int maxPending;
    private final int maxSnapshotBytes;
    private final MigrationJournal journal;
    private final Map<String, Migration> pending = new HashMap<>();
    private final Set<String> actors = new HashSet<>();
    private boolean closed;

    public CrossProcessMigrationCoordinator(int maxPending, int maxSnapshotBytes) {
        this(maxPending, maxSnapshotBytes, null);
    }

    /** Creates a coordinator with an optional durable journal for restart recovery. */
    public CrossProcessMigrationCoordinator(int maxPending, int maxSnapshotBytes, MigrationJournal journal) {
        if (maxPending < 1 || maxSnapshotBytes < 1) throw new IllegalArgumentException();
        this.maxPending = maxPending;
        this.maxSnapshotBytes = maxSnapshotBytes;
        this.journal = journal;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "gameframe-migration-timeouts");
            thread.setDaemon(true);
            return thread;
        });
    }
    public CrossProcessMigrationCoordinator() { this(1024, 2 * 1024 * 1024, null); }
    public synchronized int pendingCount() { return pending.size(); }
    public synchronized List<Migration> pendingMigrations() { return List.copyOf(pending.values()); }

    /** IDs must be globally unique; capacity and actor exclusion include recovery entries. */
    public synchronized Migration begin(Request request, Source source, Target target) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
        long remaining = request.deadlineMillis() - System.currentTimeMillis();
        if (request.snapshot().length > maxSnapshotBytes || remaining <= 0)
            throw new IllegalArgumentException("snapshot exceeds limit or deadline expired");
        if (closed || pending.size() >= maxPending || pending.containsKey(request.migrationId())
                || actors.contains(request.actorId()))
            throw new RejectedExecutionException("closed, full, or migration/actor already pending");

        var m = new Migration(request, source, target);
        pending.put(request.migrationId(), m);
        actors.add(request.actorId());
        m.timeout = scheduler.schedule(() -> {
            synchronized (this) {
                abort(m, Outcome.TIMEOUT, new TimeoutException("migration deadline"));
            }
        }, remaining, TimeUnit.MILLISECONDS);
        journalTransition(m, Phase.FREEZING);
        invoke(m, Phase.FREEZING, () -> source.freeze(request), (frozen, error) -> {
            if (error != null || frozen == null) {
                recover(m, error == null ? new IllegalStateException("null freeze result") : error);
            } else if (!frozen) {
                finish(m, Outcome.REJECTED, null);
            } else if (advance(m, Phase.PREPARING)) {
                prepare(m);
            }
        });
        return m;
    }

    private void prepare(Migration m) {
        invoke(m, Phase.PREPARING, () -> m.target.prepare(m.request), (prepared, error) -> {
            if (error != null || prepared == null) {
                recover(m, error == null ? new IllegalStateException("null prepare result") : error);
            } else if (!prepared) {
                abort(m, Outcome.REJECTED, null);
            } else {
                m.phase = Phase.PREPARED;
                journalTransition(m, Phase.PREPARED);
                if (advance(m, Phase.RELEASING)) {
                    invoke(m, Phase.RELEASING, () -> m.source.release(m.request), (ignored, releaseError) -> {
                        if (releaseError != null) recover(m, releaseError);
                        else if (advance(m, Phase.COMMITTING)) {
                            invoke(m, Phase.COMMITTING, () -> m.target.commit(m.request), (done, commitError) -> {
                                if (commitError != null) recover(m, commitError);
                                else finish(m, Outcome.ACCEPTED, null);
                            });
                        }
                    });
                }
            }
        });
    }

    /** All transitions and adapter completions are serialized by the coordinator monitor. */
    private <T> void invoke(Migration m, Phase expected, Supplier<CompletionStage<T>> operation,
                            BiConsumer<T, Throwable> completion) {
        if (m.phase != expected) return;
        m.inFlight++;
        CompletionStage<T> stage;
        try {
            stage = Objects.requireNonNull(operation.get(), "adapter returned null stage");
        } catch (Throwable error) {
            m.inFlight--;
            recover(m, error);
            return;
        }
        stage.whenComplete((value, error) -> {
            synchronized (this) {
                m.inFlight--;
                if (m.phase != expected) return;
                try {
                    completion.accept(value, unwrap(error));
                } catch (Throwable callbackError) {
                    recover(m, callbackError);
                }
            }
        });
    }

    private boolean advance(Migration m, Phase next) {
        if (System.currentTimeMillis() >= m.request.deadlineMillis()) {
            abort(m, Outcome.TIMEOUT, new TimeoutException("migration deadline"));
            return false;
        }
        m.phase = next;
        journalTransition(m, next);
        return true;
    }

    private void abort(Migration m, Outcome outcome, Throwable error) {
        if (m.phase == Phase.DONE || m.phase == Phase.RECOVERY_REQUIRED) return;
        if (m.inFlight > 0 || m.phase == Phase.FREEZING || m.phase == Phase.RELEASING
                || m.phase == Phase.COMMITTING || m.phase == Phase.ABORTING) {
            recover(m, error == null ? new IllegalStateException("migration outcome uncertain") : error);
            return;
        }
        m.phase = Phase.ABORTING;
        journalTransition(m, Phase.ABORTING);
        invoke(m, Phase.ABORTING, () -> m.target.cancel(m.request), (ignored, cancelError) -> {
            if (cancelError != null) {
                recover(m, cancelError);
                return;
            }
            invoke(m, Phase.ABORTING, () -> m.source.resume(m.request), (resumed, resumeError) -> {
                if (resumeError != null) recover(m, resumeError);
                else finish(m, outcome, error);
            });
        });
    }

    private void recover(Migration m, Throwable error) {
        if (m.phase == Phase.DONE || m.phase == Phase.RECOVERY_REQUIRED) return;
        m.phase = Phase.RECOVERY_REQUIRED;
        m.recoveryError = error;
        cancelTimeout(m);
        journalTransition(m, Phase.RECOVERY_REQUIRED);
        if (journal == null) {
            m.result.complete(new Result(m.request, Outcome.RECOVERY_REQUIRED, error));
        } else {
            m.journalTail.whenComplete((ignored, journalError) -> m.result.complete(
                    new Result(m.request, Outcome.RECOVERY_REQUIRED,
                            error != null ? error : (journalError == null ? null : unwrap(journalError)))));
        }
    }

    /** Call only after externally reconciling both nodes and fencing delayed commands. */
    public synchronized void acknowledgeRecovery(Migration m) {
        Objects.requireNonNull(m, "migration");
        if (pending.get(m.request.migrationId()) != m || m.phase != Phase.RECOVERY_REQUIRED || m.inFlight != 0)
            throw new IllegalStateException("migration is not quiescent and awaiting recovery");
        m.phase = Phase.DONE;
        cancelTimeout(m);
        journalTransition(m, Phase.DONE);
        remove(m);
        deleteJournalAfter(m);
    }

    private void finish(Migration m, Outcome outcome, Throwable error) {
        if (m.phase == Phase.DONE || m.phase == Phase.RECOVERY_REQUIRED) return;
        m.phase = Phase.DONE;
        cancelTimeout(m);
        journalTransition(m, Phase.DONE);
        remove(m);
        if (journal == null) {
            m.result.complete(new Result(m.request, outcome, error));
        } else {
            m.journalTail.whenComplete((ignored, journalError) ->
                    journal.delete(m.request.migrationId(), m.journalVersion)
                            .whenComplete((deleted, deleteError) -> m.result.complete(new Result(m.request, outcome,
                                    error != null ? error : (journalError != null ? unwrap(journalError) :
                                            deleteError != null ? unwrap(deleteError) : null)))));
        }
    }

    private void journalTransition(Migration m, Phase phase) {
        if (journal == null) return;
        long expectedVersion = m.journalVersion;
        long nextVersion = ++m.journalVersion;
        var entry = new MigrationJournal.Entry(m.request.migrationId(), m.request.actorId(),
                m.request.sourceInstance(), m.request.targetInstance(), m.request.fencingToken(), phase,
                nextVersion, System.currentTimeMillis(), m.request.snapshot());
        m.journalTail = m.journalTail.handle((ignored, previousError) -> null).thenCompose(ignored -> {
            try {
                CompletionStage<Boolean> write = expectedVersion == 0
                        ? journal.create(entry)
                        : journal.compareAndSet(entry.migrationId(), expectedVersion, entry);
                return write.thenAccept(ok -> {
                    if (!Boolean.TRUE.equals(ok)) throw new IllegalStateException("migration journal fencing rejected");
                });
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
        }).whenComplete((ignored, error) -> {
            if (error != null) {
                synchronized (CrossProcessMigrationCoordinator.this) {
                    if (m.phase != Phase.DONE && m.phase != Phase.RECOVERY_REQUIRED) {
                        recover(m, unwrap(error));
                    }
                }
            }
        });
    }

    private void deleteJournalAfter(Migration m) {
        if (journal == null) return;
        m.journalTail.whenComplete((ignored, journalError) ->
                journal.delete(m.request.migrationId(), m.journalVersion));
    }

    private void remove(Migration m) {
        pending.remove(m.request.migrationId(), m);
        actors.remove(m.request.actorId());
    }
    private static void cancelTimeout(Migration m) {
        if (m.timeout != null) m.timeout.cancel(false);
    }
    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
    }
    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        for (Migration m : List.copyOf(pending.values()))
            abort(m, Outcome.CLOSED, new IllegalStateException("migration coordinator closed"));
        scheduler.shutdownNow();
    }
}
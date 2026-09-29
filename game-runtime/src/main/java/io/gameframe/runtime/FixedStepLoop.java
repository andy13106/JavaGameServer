package io.gameframe.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongConsumer;

/** Coalesced wakeups on a shared bounded timer; state and simulation run in the actor mailbox. */
public final class FixedStepLoop implements AutoCloseable {
    private final AtomicReference<SharedTickScheduler.Handle> timer = new AtomicReference<>();
    private final AtomicBoolean pending = new AtomicBoolean(), closed = new AtomicBoolean();
    private final AtomicLong skipped = new AtomicLong(), rejected = new AtomicLong();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final ActorSystem.ActorRef actor;
    private final long nanos;
    private final int maxCatchUp;
    private long last;

    public FixedStepLoop(ActorSystem.ActorRef actor, Duration step, int maxCatchUp, LongConsumer simulate) {
        this(actor, step, maxCatchUp, simulate, SharedTickScheduler.global());
    }

    public FixedStepLoop(ActorSystem.ActorRef actor, Duration step, int maxCatchUp,
                         LongConsumer simulate, SharedTickScheduler scheduler) {
        this.actor = Objects.requireNonNull(actor);
        this.nanos = step.toNanos();
        this.maxCatchUp = maxCatchUp;
        Objects.requireNonNull(simulate);
        Objects.requireNonNull(scheduler);
        if (nanos < 1_000_000 || maxCatchUp < 1) throw new IllegalArgumentException();
        last = System.nanoTime();
        var handle = scheduler.schedule(step, () -> {
            if (closed.get() || !pending.compareAndSet(false, true)) return;
            actor.tell(() -> {
                if (closed.get()) return;
                long now = System.nanoTime();
                long due = Math.max(0, (now - last) / nanos);
                long count = Math.min(due, maxCatchUp);
                for (long i = 0; i < count; i++) simulate.accept(nanos);
                skipped.addAndGet(due - count);
                last += due * nanos;
            }).whenComplete((v, error) -> {
                if (error != null) {
                    Throwable cause = error instanceof CompletionException ? error.getCause() : error;
                    if (cause instanceof RejectedExecutionException) rejected.incrementAndGet();
                    else { failure.compareAndSet(null, cause); close(); }
                }
                pending.set(false);
            });
        });
        timer.set(handle);
        if (closed.get()) handle.close();
        actor.stopped().whenComplete((v, e) -> close());
    }

    public java.util.Optional<Throwable> failure() { return java.util.Optional.ofNullable(failure.get()); }
    public long skippedSteps() { return skipped.get(); }
    public long rejectedWakeups() { return rejected.get(); }
    public void close() {
        closed.set(true);
        var handle = timer.get();
        if (handle != null) handle.close();
    }
}

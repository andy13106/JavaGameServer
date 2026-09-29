package io.gameframe.scene;

import com.zfoo.event.anno.Bus;
import com.zfoo.event.enhance.IEventReceiver;
import com.zfoo.event.manager.EventBus;
import com.zfoo.event.model.IEvent;
import io.gameframe.runtime.ActorSystem;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

/** zfoo event subscriptions whose handlers run through bounded actor mailboxes. Publish immutable events. */
public final class ActorEvents implements AutoCloseable {
    public record Stats(int subscriptions, long delivered, long rejected, long failed, long discarded) {}
    private final Set<Subscription> subscriptions = new HashSet<>();
    private final int maxSubscriptions;
    private final AtomicLong delivered = new AtomicLong(), rejected = new AtomicLong(),
        failed = new AtomicLong(), discarded = new AtomicLong();
    private boolean closed;
    public ActorEvents(int maxSubscriptions) {
        if (maxSubscriptions < 1) throw new IllegalArgumentException();
        this.maxSubscriptions = maxSubscriptions;
    }
    public Scope scope(ActorSystem.ActorRef actor) { return new Scope(Objects.requireNonNull(actor)); }
    public void post(IEvent event) { EventBus.post(Objects.requireNonNull(event)); }
    public synchronized Stats stats() {
        return new Stats(subscriptions.size(), delivered.get(), rejected.get(), failed.get(), discarded.get());
    }
    public final class Scope implements AutoCloseable {
        private final ActorSystem.ActorRef actor;
        private final Set<Subscription> owned = new HashSet<>();
        private boolean scopeClosed;
        private Scope(ActorSystem.ActorRef actor) { this.actor = actor; }
        public <E extends IEvent> Subscription subscribe(Class<E> type, Consumer<? super E> listener) {
            actor.requireCurrent(); Objects.requireNonNull(type); Objects.requireNonNull(listener);
            synchronized (this) {
                if (scopeClosed) throw new IllegalStateException("event scope closed");
                synchronized (ActorEvents.this) {
                    if (closed || subscriptions.size() >= maxSubscriptions)
                        throw new RejectedExecutionException("subscription capacity/closed");
                    var subscription = new Subscription(this, type, event -> listener.accept(type.cast(event)));
                    subscriptions.add(subscription); owned.add(subscription);
                    EventBus.registerEventReceiver(type, subscription);
                    return subscription;
                }
            }
        }
        @Override public void close() {
            List<Subscription> snapshot;
            synchronized (this) {
                if (scopeClosed) return;
                scopeClosed = true; snapshot = List.copyOf(owned); owned.clear();
            }
            snapshot.forEach(Subscription::close);
        }
    }
    public final class Subscription implements AutoCloseable, IEventReceiver {
        private final Scope scope;
        private final Class<? extends IEvent> type;
        private final Consumer<IEvent> listener;
        private final AtomicBoolean active = new AtomicBoolean(true);
        private Subscription(Scope scope, Class<? extends IEvent> type, Consumer<IEvent> listener) {
            this.scope = scope; this.type = type; this.listener = listener;
        }
        @Override public Bus bus() { return Bus.CurrentThread; }
        @Override public Object getBean() { return this; }
        @Override public void invoke(IEvent event) {
            if (!active.get()) return;
            var invoked = new AtomicBoolean();
            scope.actor.tell(() -> {
                if (!active.get()) { discarded.incrementAndGet(); return; }
                invoked.set(true);
                try { listener.accept(event); delivered.incrementAndGet(); }
                catch (RuntimeException | Error error) { failed.incrementAndGet(); throw error; }
            }).whenComplete((v, error) -> {
                if (error == null || invoked.get()) return;
                Throwable cause = error instanceof CompletionException ? error.getCause() : error;
                if (cause instanceof RejectedExecutionException) rejected.incrementAndGet();
                else failed.incrementAndGet();
            });
        }
        /** Queued deliveries re-check this flag; a handler already running may finish. */
        @Override public void close() {
            synchronized (ActorEvents.this) {
                if (!active.compareAndSet(true, false)) return;
                EventBus.unregisterEventReceiver(type, this);
                subscriptions.remove(this);
            }
            synchronized (scope) { scope.owned.remove(this); }
        }
    }
    @Override public void close() {
        List<Subscription> snapshot;
        synchronized (this) { if (closed) return; closed = true; snapshot = List.copyOf(subscriptions); }
        snapshot.forEach(Subscription::close);
    }
}

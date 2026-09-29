package io.gameframe.transport;

import com.zfoo.net.session.Session;
import io.gameframe.runtime.ActorSystem;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binds zfoo sessions to GameFrame actors without changing vendor zfoo code.
 * zfoo active/inactive handlers call bind/disconnect; packet receivers call dispatch.
 * A binding is generation-safe: a late callback from an old session cannot enter a replacement actor.
 */
public final class GameSessionRouter implements AutoCloseable {
    public record Binding(long generation, String actorId, Session session, ActorSystem.ActorRef actor) {}

    private final ActorSystem actors;
    private final AtomicLong generations = new AtomicLong();
    private final Map<Long, Binding> bySid = new HashMap<>();
    private final Map<String, Binding> byActor = new HashMap<>();
    private final WeakHashMap<Session, Boolean> retiredSessions = new WeakHashMap<>();
    private final Object lock = new Object();

    public GameSessionRouter(int actorThreads, int maxActors, int actorCapacity, int quantum) {
        actors = new ActorSystem(actorThreads, maxActors, actorCapacity, quantum);
    }

    /** Bind one active network session to one logical actor. Reconnects must disconnect first. */
    public Binding bind(Session session, String actorId) {
        Objects.requireNonNull(session);
        if (actorId == null || actorId.isBlank()) throw new IllegalArgumentException("actor id required");
        synchronized (lock) {
            if (bySid.containsKey(session.getSid())) throw new IllegalStateException("session already bound");
            if (byActor.containsKey(actorId)) throw new IllegalStateException("actor already bound: " + actorId);
            ActorSystem.ActorRef actor = actors.spawn(actorId);
            Binding binding = new Binding(generations.incrementAndGet(), actorId, session, actor);
            bySid.put(session.getSid(), binding);
            retiredSessions.remove(session);
            byActor.put(actorId, binding);
            return binding;
        }
    }

    /** Return the current binding for a session, if it is still active. */
    public Optional<Binding> binding(Session session) {
        Objects.requireNonNull(session);
        synchronized (lock) {
            Binding binding = bySid.get(session.getSid());
            return binding != null && binding.session() == session ? Optional.of(binding) : Optional.empty();
        }
    }

    /** Return the current binding for a logical actor, if any. */
    public Optional<Binding> bindingByActor(String actorId) {
        Objects.requireNonNull(actorId);
        synchronized (lock) {
            return Optional.ofNullable(byActor.get(actorId));
        }
    }

    /** Returns true for a current binding or a retired session that must be dropped. */
    public boolean isManaged(Session session) {
        Objects.requireNonNull(session);
        synchronized (lock) {
            Binding binding = bySid.get(session.getSid());
            return (binding != null && binding.session() == session) || retiredSessions.containsKey(session);
        }
    }

    /** Route synchronous game work to the session's actor mailbox. */
    public <T> CompletionStage<T> dispatch(Session session, Callable<T> action) {
        Objects.requireNonNull(session);
        Objects.requireNonNull(action);
        Binding binding;
        synchronized (lock) {
            binding = bySid.get(session.getSid());
            if (binding == null || binding.session() != session)
                return CompletableFuture.failedFuture(new RejectedExecutionException("session is not bound"));
        }
        return binding.actor().call(() -> {
            requireCurrent(binding);
            return action.call();
        });
    }

    /** Remove a session and stop its actor. A late disconnect from an old session is ignored. */
    public CompletionStage<Void> disconnect(Session session) {
        Objects.requireNonNull(session);
        Binding binding;
        synchronized (lock) {
            binding = bySid.get(session.getSid());
            if (binding == null || binding.session() != session) return CompletableFuture.completedFuture(null);
            bySid.remove(session.getSid());
            retiredSessions.put(session, Boolean.TRUE);
            byActor.remove(binding.actorId(), binding);
        }
        return binding.actor().stop();
    }

    public int boundCount() {
        synchronized (lock) { return bySid.size(); }
    }

    public List<ActorSystem.ActorStats> actorStats() { return actors.stats(); }

    private void requireCurrent(Binding binding) {
        synchronized (lock) {
            if (bySid.get(binding.session().getSid()) != binding)
                throw new RejectedExecutionException("session binding is stale");
        }
    }

    @Override public void close() { actors.close(); }
}


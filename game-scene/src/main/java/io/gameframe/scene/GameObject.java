package io.gameframe.scene;

import io.gameframe.runtime.ActorSystem;
import java.util.*;

/** Actor-owned component container. It does not pool entities or persist their state. */
public final class GameObject implements AutoCloseable {
    private final long id;
    private final ActorSystem.ActorRef actor;
    private final ActiveComponentScheduler scheduler;
    private final Map<Integer, GameComponent> components = new LinkedHashMap<>();
    private boolean closed, changingLifecycle;
    private ActorEvents eventBus;
    private ActorEvents.Scope eventScope;

    public GameObject(long id, ActiveComponentScheduler scheduler) {
        if (id <= 0) throw new IllegalArgumentException("positive entity ID required");
        this.id = id;
        this.scheduler = Objects.requireNonNull(scheduler);
        this.actor = scheduler.actor();
        requireCurrent();
    }
    public long id() { return id; }
    public ActorSystem.ActorRef actor() { return actor; }
    public ActorEvents.Scope events(ActorEvents bus) {
        requireCurrent(); Objects.requireNonNull(bus);
        if (closed) throw new IllegalStateException("object closed");
        if (eventScope == null) { eventBus = bus; eventScope = bus.scope(actor); }
        else if (eventBus != bus) throw new IllegalArgumentException("object already uses another event bus");
        return eventScope;
    }
    public void requireCurrent() { actor.requireCurrent(); }
    ActiveComponentScheduler scheduler() { return scheduler; }

    public <T extends GameComponent> T add(T component) {
        requireChange();
        Objects.requireNonNull(component);
        if (!component.detached()) throw new IllegalStateException("component already has an owner");
        if (components.containsKey(component.majorType())) throw new IllegalArgumentException("duplicate major slot");
        changingLifecycle = true;
        components.put(component.majorType(), component);
        try {
            component.attach(this);
            scheduler.configure(component, component.updatePolicy());
            return component;
        } catch (RuntimeException | Error failure) {
            components.remove(component.majorType());
            scheduler.unregister(component);
            try { component.detach(); }
            catch (RuntimeException | Error cleanup) { if (failure != cleanup) failure.addSuppressed(cleanup); }
            throw failure;
        } finally { changingLifecycle = false; }
    }
    public Optional<GameComponent> component(int majorType) {
        requireCurrent();
        return Optional.ofNullable(components.get(majorType));
    }
    public List<GameComponent> components() { requireCurrent(); return List.copyOf(components.values()); }
    public boolean remove(int majorType) {
        requireChange();
        GameComponent component = components.remove(majorType);
        if (component == null) return false;
        changingLifecycle = true;
        scheduler.unregister(component);
        try { component.detach(); }
        finally { changingLifecycle = false; }
        return true;
    }
    private void requireChange() {
        requireCurrent();
        if (closed) throw new IllegalStateException("object closed");
        if (changingLifecycle) throw new IllegalStateException("recursive lifecycle mutation");
    }
    @Override public void close() {
        requireCurrent();
        if (closed) return;
        requireChange();
        closed = true;
        if (eventScope != null) eventScope.close();
        changingLifecycle = true;
        var detached = List.copyOf(components.values());
        components.clear();
        detached.forEach(scheduler::unregister);
        Throwable failure = null;
        try {
            for (var component : detached) {
                try { component.detach(); }
                catch (RuntimeException | Error error) {
                    if (failure == null) failure = error;
                    else if (failure != error) failure.addSuppressed(error);
                }
            }
        } finally { changingLifecycle = false; }
        if (failure instanceof RuntimeException error) throw error;
        if (failure instanceof Error error) throw error;
    }
}

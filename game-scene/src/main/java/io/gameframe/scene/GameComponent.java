package io.gameframe.scene;

import java.util.Objects;

/** Type ID = (major slot << 8) | subtype. One component per major slot on an object. */
public abstract class GameComponent {
    private enum State { DETACHED, ATTACHING, ATTACHED, DETACHING }
    private final int typeId;
    private GameObject owner;
    private State state = State.DETACHED;
    private UpdatePolicy policy = UpdatePolicy.passive();
    private ActorEvents eventBus;
    private ActorEvents.Scope eventScope;

    protected GameComponent(int typeId) {
        if (typeId < 256 || typeId > 65535) throw new IllegalArgumentException("typeId must have major slot 1..255");
        this.typeId = typeId;
    }
    public final int typeId() { return typeId; }
    public final int majorType() { return typeId >>> 8; }
    public final UpdatePolicy updatePolicy() {
        if (owner != null) owner.requireCurrent();
        return policy;
    }
    /** Detached components must remain exclusively owned by their creator. */
    public final void updatePolicy(UpdatePolicy next) {
        Objects.requireNonNull(next);
        if (owner != null) owner.requireCurrent();
        if (policy.equals(next)) return;
        if (state == State.ATTACHED) owner.scheduler().configure(this, next);
        policy = next;
    }
    protected final GameObject owner() {
        if (owner == null) throw new IllegalStateException("component is detached");
        owner.requireCurrent(); return owner;
    }
    /** Component-owned subscriptions are cancelled on detach, including failed attach rollback. */
    protected final ActorEvents.Scope events(ActorEvents bus) {
        var object = owner(); Objects.requireNonNull(bus);
        if (state == State.DETACHING) throw new IllegalStateException("component detaching");
        if (eventScope == null) { eventBus = bus; eventScope = bus.scope(object.actor()); }
        else if (eventBus != bus) throw new IllegalArgumentException("component already uses another event bus");
        return eventScope;
    }
    protected void onAttach() {}
    protected void onDetach() {}
    protected void onUpdate(long elapsedNanos) {}

    final boolean detached() { return state == State.DETACHED; }
    final void attach(GameObject next) {
        if (!detached()) throw new IllegalStateException("already attached");
        owner = next; state = State.ATTACHING;
        onAttach(); state = State.ATTACHED;
    }
    final void detach() {
        if (state == State.DETACHED) return;
        state = State.DETACHING;
        try {
            if (eventScope != null) eventScope.close();
            onDetach();
        } finally { eventScope = null; eventBus = null; owner = null; state = State.DETACHED; }
    }
    @Override public final boolean equals(Object other) { return this == other; }
    @Override public final int hashCode() { return System.identityHashCode(this); }
}

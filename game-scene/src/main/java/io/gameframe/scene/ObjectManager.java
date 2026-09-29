package io.gameframe.scene;
import io.gameframe.base.ObjectRegistry;
import java.util.*;

/** Actor-owned bounded object registry. Close from the actor's onStop hook. */
public final class ObjectManager implements AutoCloseable {
    private final ObjectRegistry<Long, GameObject> objects = new ObjectRegistry<>();
    private final ActiveComponentScheduler scheduler;
    private final ComponentFactory factory;
    private final int capacity;
    private boolean closed, mutating;
    public ObjectManager(ActiveComponentScheduler scheduler, ComponentFactory factory, int capacity) {
        this.scheduler = Objects.requireNonNull(scheduler); this.factory = Objects.requireNonNull(factory);
        if (capacity < 1) throw new IllegalArgumentException();
        this.capacity = capacity; scheduler.actor().requireCurrent();
    }
    public GameObject create(long id, List<ComponentFactory.Spec> components) {
        checkMutation();
        if (objects.contains(id)) throw new IllegalArgumentException("duplicate object");
        if (objects.size() >= capacity) throw new java.util.concurrent.RejectedExecutionException("object capacity");
        var specs = List.copyOf(components);
        var majors = new HashSet<Integer>();
        for (var spec : specs) if (!majors.add(spec.typeId() >>> 8)) throw new IllegalArgumentException("duplicate major slot");
        mutating = true;
        GameObject object = null;
        try {
            object = new GameObject(id, scheduler);
            for (var spec : specs) object.add(factory.create(spec));
            if (!objects.add(id, object)) throw new IllegalStateException("object already registered");
            return object;
        } catch (RuntimeException | Error error) {
            if (object != null) try { object.close(); }
            catch (RuntimeException | Error cleanup) { if (error != cleanup) error.addSuppressed(cleanup); }
            throw error;
        } finally { mutating = false; }
    }
    public Optional<GameObject> find(long id) { scheduler.actor().requireCurrent(); return objects.find(id); }
    public List<GameObject> snapshot() { scheduler.actor().requireCurrent(); return List.copyOf(objects.snapshot().values()); }
    public int size() { scheduler.actor().requireCurrent(); return objects.size(); }
    public boolean remove(long id) {
        checkMutation(); var object = objects.find(id);
        if (object.isEmpty()) return false;
        mutating = true;
        try { objects.remove(id); object.get().close(); return true; }
        finally { mutating = false; }
    }
    private void checkMutation() {
        scheduler.actor().requireCurrent();
        if (closed || mutating) throw new IllegalStateException("manager closed/recursive mutation");
    }
    @Override public void close() {
        scheduler.actor().requireCurrent();
        if (closed) return;
        checkMutation(); closed = true; mutating = true;
        var snapshot = objects.snapshot(); objects.clear();
        Throwable failure = null;
        try {
            for (var object : snapshot.values()) try { object.close(); }
            catch (RuntimeException | Error e) {
                if (failure == null) failure = e; else if (failure != e) failure.addSuppressed(e);
            }
        } finally { mutating = false; }
        if (failure instanceof RuntimeException e) throw e;
        if (failure instanceof Error e) throw e;
    }
}

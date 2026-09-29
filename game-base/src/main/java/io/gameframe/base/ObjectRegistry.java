package io.gameframe.base;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Thread-safe identity registry. Snapshots and callbacks are detached from the map,
 * so a callback may add or remove other entries without holding the registry lock.
 */
public final class ObjectRegistry<K, V> {
    private final ConcurrentHashMap<K, V> objects = new ConcurrentHashMap<>();

    public boolean add(K key, V value) {
        require(key, value);
        return objects.putIfAbsent(key, value) == null;
    }

    /** Creator runs outside map locks, may run multiple times under contention; avoid external side effects. */
    public V emplace(K key, Supplier<? extends V> creator) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(creator, "creator");
        V existing = objects.get(key);
        if (existing != null) return existing;
        V created = Objects.requireNonNull(creator.get(), "creator returned null");
        V raced = objects.putIfAbsent(key, created);
        return raced == null ? created : raced;
    }

    public Optional<V> find(K key) { return Optional.ofNullable(objects.get(key)); }
    public boolean contains(K key) { return objects.containsKey(key); }
    public boolean remove(K key) { return objects.remove(key) != null; }
    public void clear() { objects.clear(); }
    public int size() { return objects.size(); }
    public boolean isEmpty() { return objects.isEmpty(); }

    /** Detached, shallow snapshot; mutable values still require their own ownership rules. */
    public Map<K, V> snapshot() { return Map.copyOf(objects); }

    public void forEach(Consumer<? super Map.Entry<K, V>> callback) {
        Objects.requireNonNull(callback, "callback");
        for (var entry : snapshot().entrySet()) callback.accept(entry);
    }

    public boolean update(K key, Consumer<? super V> callback) {
        Objects.requireNonNull(callback, "callback");
        V value = objects.get(key);
        if (value == null) return false;
        callback.accept(value);
        return true;
    }

    private static void require(Object key, Object value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
    }
}
package io.gameframe.base;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/** Thread-safe creator registry. Creators run outside the registry map operation. */
public final class Factory<K, V> {
    private final ConcurrentHashMap<K, Supplier<? extends V>> creators = new ConcurrentHashMap<>();

    public boolean register(K key, Supplier<? extends V> creator) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(creator, "creator");
        return creators.putIfAbsent(key, creator) == null;
    }

    public boolean replace(K key, Supplier<? extends V> creator) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(creator, "creator");
        creators.put(key, creator);
        return true;
    }

    public boolean unregister(K key) { return creators.remove(key) != null; }
    public boolean contains(K key) { return creators.containsKey(key); }
    public Optional<V> create(K key) {
        Supplier<? extends V> creator = creators.get(key);
        return creator == null ? Optional.empty() : Optional.ofNullable(creator.get());
    }
    public int size() { return creators.size(); }
}
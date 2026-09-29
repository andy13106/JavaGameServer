package io.gameframe.base;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Thread-safe cache of short-lived objects; maxCached limits idle objects, not live leases.
 * A Lease and its borrowed value are exclusively owned by the borrower. Never retain the value after close.
 * close resets and recycles it when capacity permits; a failed reset discards the object.
 */
public final class ObjectPool<T> {
    private final ArrayBlockingQueue<T> cached;
    private final Supplier<? extends T> creator;
    private final Consumer<? super T> resetter;

    public ObjectPool(int maxCached, Supplier<? extends T> creator, Consumer<? super T> resetter) {
        if (maxCached < 1) throw new IllegalArgumentException("maxCached must be positive");
        this.cached = new ArrayBlockingQueue<>(maxCached);
        this.creator = Objects.requireNonNull(creator, "creator");
        this.resetter = Objects.requireNonNull(resetter, "resetter");
    }

    public Lease acquire() {
        T value = cached.poll();
        if (value == null) value = Objects.requireNonNull(creator.get(), "creator returned null");
        return new Lease(value);
    }

    public int cachedSize() { return cached.size(); }

    public final class Lease implements AutoCloseable {
        private final T value;
        private final AtomicBoolean closed = new AtomicBoolean();
        private Lease(T value) { this.value = value; }
        public T value() {
            if (closed.get()) throw new IllegalStateException("lease is closed");
            return value;
        }
        @Override public void close() {
            if (!closed.compareAndSet(false, true)) return;
            resetter.accept(value);
            cached.offer(value);
        }
    }
}
package io.gameframe.base;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class BasePrimitivesTest {
    @Test void registrySnapshotAndIterationCannotMutateRegistryThroughEntries() {
        var registry = new ObjectRegistry<Integer, String>();
        assertTrue(registry.add(1, "one"));
        assertFalse(registry.add(1, "again"));
        var snapshot = registry.snapshot();
        registry.forEach(entry -> {
            assertThrows(UnsupportedOperationException.class, () -> entry.setValue("bypass"));
            registry.remove(entry.getKey());
            registry.add(2, "two");
        });
        assertEquals("one", snapshot.get(1));
        assertFalse(snapshot.containsKey(2));
        assertEquals("two", registry.find(2).orElseThrow());
        assertTrue(registry.update(2, value -> registry.remove(2)));
        assertTrue(registry.isEmpty());
    }

    @Test void concurrentCreatorsMayRunButAllCallersReceiveTheRegisteredObject() throws Exception {
        var registry = new ObjectRegistry<String, Object>();
        var creatorsEntered = new CountDownLatch(8);
        var releaseCreators = new CountDownLatch(1);
        var calls = new AtomicInteger();
        var pool = Executors.newFixedThreadPool(8);
        try {
            var results = new ArrayList<Future<Object>>();
            for (int i = 0; i < 8; i++) results.add(pool.submit(() -> registry.emplace("x", () -> {
                calls.incrementAndGet();
                creatorsEntered.countDown();
                try { assertTrue(releaseCreators.await(3, TimeUnit.SECONDS)); }
                catch (InterruptedException e) { throw new RuntimeException(e); }
                return new Object();
            })));
            try { assertTrue(creatorsEntered.await(3, TimeUnit.SECONDS)); }
            finally { releaseCreators.countDown(); }
            Object registered = results.getFirst().get(3, TimeUnit.SECONDS);
            for (var result : results) assertSame(registered, result.get(3, TimeUnit.SECONDS));
            assertSame(registered, registry.find("x").orElseThrow());
            assertEquals(8, calls.get());
            assertEquals(1, registry.size());
        } finally { releaseCreators.countDown(); pool.shutdownNow(); }
    }

    @Test void factoryCreatorsCanRegisterOtherTypesAndAreNotCached() {
        var factory = new Factory<String, Object>();
        assertTrue(factory.register("x", () -> {
            factory.register("y", Object::new);
            return new Object();
        }));
        assertNotSame(factory.create("x").orElseThrow(), factory.create("x").orElseThrow());
        assertTrue(factory.contains("y"));
        assertFalse(factory.register("x", Object::new));
        assertTrue(factory.unregister("x"));
        assertTrue(factory.create("x").isEmpty());
    }

    @Test void poolResetsAndReusesOnlyOnceAndEnforcesCacheCapacity() {
        var resets = new AtomicInteger();
        var pool = new ObjectPool<>(1, StringBuilder::new, value -> { value.setLength(0); resets.incrementAndGet(); });
        var first = pool.acquire();
        var second = pool.acquire();
        StringBuilder value = first.value();
        value.append("data");
        first.close();
        first.close();
        second.close();
        assertEquals(2, resets.get());
        assertEquals(1, pool.cachedSize());
        assertThrows(IllegalStateException.class, first::value);
        try (var reused = pool.acquire()) {
            assertSame(value, reused.value());
            assertEquals(0, reused.value().length());
        }
    }

    @Test void resetFailureDiscardsTheObjectInsteadOfRecyclingDirtyState() {
        var pool = new ObjectPool<>(1, StringBuilder::new, value -> {
            if (!value.isEmpty()) throw new IllegalStateException("cannot reset");
        });
        var broken = pool.acquire();
        Object old = broken.value();
        broken.value().append("dirty");
        assertThrows(IllegalStateException.class, broken::close);
        assertEquals(0, pool.cachedSize());
        broken.close();
        try (var next = pool.acquire()) { assertNotSame(old, next.value()); }
    }
}

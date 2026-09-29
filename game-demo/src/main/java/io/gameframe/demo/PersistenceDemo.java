package io.gameframe.demo;

import io.gameframe.runtime.*;
import io.gameframe.scene.*;
import io.gameframe.storage.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Component persistence: coalescing, periodic writes, full checkpoint, and a draining shutdown. */
public final class PersistenceDemo {
    public record Summary(int mergedEdits, int pendingBeforePeriodic, int writeCalls,
                          long coreVersion, long bagVersion, long gold, long slots, int dirtyAfterShutdown) {}
    private static <T> T get(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
    private static final class Core extends GameComponent {
        final DirtyDocumentSet storage; long gold = 100, level = 1;
        Core(DirtyDocumentSet storage) { super(256); this.storage = storage; }
        void reward(long amount) {
            owner(); long next = Math.addExact(gold, amount);
            storage.markPatch("core", new DocumentPatch(Map.of(), Set.of(), Map.of("gold", amount)));
            gold = next;
        }
        void checkpoint(long nextLevel) {
            owner(); long revision = storage.localView("core").revision();
            storage.markReplace("core", revision, Map.of("gold", gold, "level", nextLevel)); level = nextLevel;
        }
    }
    private static final class Bag extends GameComponent {
        final DirtyDocumentSet storage; long slots = 2;
        Bag(DirtyDocumentSet storage) { super(512); this.storage = storage; }
        void expand(long next) {
            owner(); storage.markPatch("bag", DocumentPatch.set("slots", next)); slots = next;
        }
    }
    public static Summary run() throws Exception {
        try (var store = new MemoryDocumentStore(2, 64)) { return run(store); }
    }
    /** The caller owns the supplied store and test namespace. */
    public static Summary run(DocumentStore delegate) throws Exception {
        var writes = new AtomicInteger();
        DocumentStore store = new DocumentStore() {
            public CompletionStage<Optional<Snapshot>> load(StoreKey key) { return delegate.load(key); }
            public CompletionStage<WriteResult> write(StoreKey key, WriteCommand command) {
                writes.incrementAndGet(); return delegate.write(key, command);
            }
            public CompletionStage<Void> flush(StoreKey key) { return delegate.flush(key); }
            public void close() {}
        };
        try (var timer = new SharedTickScheduler(1, 8, "persistence-demo");
             var actors = new ActorSystem(2, 1, 32, 8)) {
            var actor = actors.spawn("persistent-player");
            var dirty = get(actor.call(() -> new DirtyDocumentSet(store, "player_component",
                UUID.randomUUID().toString(), 4, 32, actor::requireCurrent)));
            var opening = get(actor.call(() -> CompletableFuture.allOf(
                dirty.open("core").toCompletableFuture(), dirty.open("bag").toCompletableFuture())));
            get(opening);
            var player = get(actor.call(() -> {
                dirty.markReplace("core", Map.of("gold", 100L, "level", 1L));
                dirty.markReplace("bag", Map.of("slots", 2L));
                var object = new GameObject(1, new ActiveComponentScheduler(actor, 8, 4));
                object.add(new Core(dirty)); object.add(new Bag(dirty)); return object;
            }));
            if (!get(get(actor.call(dirty::flush))).complete()) throw new IllegalStateException("initial persistence failed");
            var periodic = new CompletableFuture<DirtyDocumentSet.FlushResult>();
            int before = get(actor.call(() -> {
                var core = (Core) player.component(1).orElseThrow();
                for (int i = 0; i < 100; i++) core.reward(1);
                ((Bag) player.component(2).orElseThrow()).expand(4);
                return dirty.pendingOperations();
            }));
            // CompletableFuture completion below only records immutable results, never mutates actor state.
            try (var loop = new FixedStepLoop(actor, Duration.ofMillis(10), 1, nanos ->
                dirty.flush().whenComplete((result, error) -> {
                    if (error != null) periodic.completeExceptionally(error);
                    else if (!result.items().isEmpty()) periodic.complete(result);
                }), timer)) {
                if (!get(periodic).complete()) throw new IllegalStateException("periodic persistence failed");
            }
            var shutdown = get(actor.call(() -> {
                var core = (Core) player.component(1).orElseThrow();
                core.checkpoint(5); core.reward(3);
                // Ingress is frozen before this stage is returned; do not stop the actor yet.
                return dirty.flushAndClose();
            }));
            if (!get(shutdown).complete()) throw new IllegalStateException("persistence shutdown requires recovery");
            int pending = dirty.pendingOperations();
            get(actor.tell(player::close)); get(actor.stop());
            var core = get(store.load(dirty.key("core"))).orElseThrow();
            var bag = get(store.load(dirty.key("bag"))).orElseThrow();
            return new Summary(100, before, writes.get(), core.version(), bag.version(),
                ((Number) core.data().get("gold")).longValue(), ((Number) bag.data().get("slots")).longValue(), pending);
        }
    }
}

package io.gameframe.demo;

import io.gameframe.runtime.ActorSystem;
import io.gameframe.storage.DocumentPatch;
import io.gameframe.storage.MemoryDocumentStore;
import io.gameframe.storage.StoreKey;
import io.gameframe.storage.WriteCommand;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Repeatable local baseline for actor dispatch and explicit partial persistence updates. */
public final class BenchmarkMain {
    public record Result(String javaVersion, String os, int processors,
                         int actorOperations, double actorSeconds, double actorOperationsPerSecond,
                         int storageOperations, double storageSeconds, double storageOperationsPerSecond) {
        public String json() {
            return String.format(Locale.ROOT,
                    "{\"javaVersion\":\"%s\",\"os\":\"%s\",\"processors\":%d," +
                            "\"actorOperations\":%d,\"actorSeconds\":%.6f,\"actorOperationsPerSecond\":%.2f," +
                            "\"storageOperations\":%d,\"storageSeconds\":%.6f,\"storageOperationsPerSecond\":%.2f}",
                    escape(javaVersion), escape(os), processors, actorOperations, actorSeconds,
                    actorOperationsPerSecond, storageOperations, storageSeconds, storageOperationsPerSecond);
        }
    }

    private BenchmarkMain() {}

    public static void runFromArgs(String[] args) throws Exception {
        int actorOperations = option(args, "--actor-ops", 200_000);
        int storageOperations = option(args, "--storage-ops", 20_000);
        System.out.println(run(actorOperations, storageOperations).json());
    }

    public static Result run(int actorOperations, int storageOperations) throws Exception {
        requireRange(actorOperations, "actorOperations", 1_000, 1_000_000);
        requireRange(storageOperations, "storageOperations", 1_000, 200_000);
        int processors = Runtime.getRuntime().availableProcessors();
        long actorNanos = actorBenchmark(actorOperations);
        long storageNanos = storageBenchmark(storageOperations);
        double actorSeconds = actorNanos / 1_000_000_000.0;
        double storageSeconds = storageNanos / 1_000_000_000.0;
        return new Result(System.getProperty("java.version"),
                System.getProperty("os.name") + " " + System.getProperty("os.version"), processors,
                actorOperations, actorSeconds, actorOperations / actorSeconds,
                storageOperations, storageSeconds, storageOperations / storageSeconds);
    }

    private static long actorBenchmark(int operations) throws Exception {
        int warmup = Math.max(1_000, operations / 10);
        var counter = new AtomicLong();
        try (var actors = new ActorSystem(Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors())),
                2, operations + warmup + 32, 64)) {
            var actor = actors.spawn("benchmark-actor");
            get(actor.started());
            dispatch(actor, counter, warmup);
            long start = System.nanoTime();
            dispatch(actor, counter, operations);
            long elapsed = System.nanoTime() - start;
            if (counter.get() != (long) warmup + operations) throw new IllegalStateException("actor work lost");
            get(actor.stop());
            return elapsed;
        }
    }

    private static void dispatch(ActorSystem.ActorRef actor, AtomicLong counter, int operations) throws Exception {
        var futures = new CompletableFuture<?>[operations];
        for (int i = 0; i < operations; i++) {
            futures[i] = actor.tell(counter::incrementAndGet).toCompletableFuture();
        }
        CompletableFuture.allOf(futures).get(60, TimeUnit.SECONDS);
    }

    private static long storageBenchmark(int operations) throws Exception {
        int warmup = Math.max(1_000, operations / 10);
        var key = new StoreKey("benchmark", "partial-update");
        var increment = new DocumentPatch(Map.of(), Set.of(), Map.of("counter", 1L));
        try (var store = new MemoryDocumentStore(Math.max(1, Math.min(4,
                Runtime.getRuntime().availableProcessors())), operations + warmup + 32)) {
            long version = get(store.write(key, WriteCommand.create("benchmark-create", Map.of("counter", 0L)))).version();
            version = patch(store, key, increment, version, warmup, "warmup");
            long start = System.nanoTime();
            version = patch(store, key, increment, version, operations, "measure");
            long elapsed = System.nanoTime() - start;
            var snapshot = get(store.load(key)).orElseThrow();
            if (snapshot.version() != version || ((Number) snapshot.data().get("counter")).longValue() != (long) warmup + operations)
                throw new IllegalStateException("storage update lost");
            return elapsed;
        }
    }

    private static long patch(MemoryDocumentStore store, StoreKey key, DocumentPatch patch, long version,
                              int operations, String prefix) throws Exception {
        for (int i = 0; i < operations; i++) {
            var result = get(store.write(key, WriteCommand.patch(version, prefix + '-' + i, patch)));
            if (!result.successful()) throw new IllegalStateException("storage write failed: " + result.status());
            version = result.version();
        }
        return version;
    }

    private static int option(String[] args, String name, int fallback) {
        for (String value : args) {
            if (value.startsWith(name + "=")) return Integer.parseInt(value.substring(name.length() + 1));
        }
        return fallback;
    }

    private static void requireRange(int value, String name, int min, int max) {
        if (value < min || value > max) throw new IllegalArgumentException(name + " must be " + min + ".." + max);
    }

    private static <T> T get(CompletionStage<T> stage) throws Exception {
        return stage.toCompletableFuture().get(60, TimeUnit.SECONDS);
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
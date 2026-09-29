package io.gameframe.storage;

import java.util.*;
import java.util.concurrent.*;

/**
 * Bounded component documents with immutable flush boundaries. Callers submit authoritative
 * game state on their actor; synchronized internal bookkeeping may run on storage workers.
 * No transaction across components, no automatic retry, and no implicit save in close().
 */
public final class DirtyDocumentSet implements AutoCloseable {
    public record Limits(int components, int operations, long bufferedBytes, long documentBytes, int patchPaths) {
        public Limits {
            if (components < 1 || operations < 1 || bufferedBytes < 1 || documentBytes < 1 || patchPaths < 1)
                throw new IllegalArgumentException("positive limits required");
        }
    }
    public record FlushItem(String component, int attempted, int applied, WriteResult.Status status, Throwable error) {
        public boolean successful() { return error == null &&
            (status == WriteResult.Status.APPLIED || status == WriteResult.Status.ALREADY_APPLIED); }
    }
    public record FlushResult(List<FlushItem> items) {
        public FlushResult { items = List.copyOf(items); }
        public boolean complete() { return items.stream().allMatch(FlushItem::successful); }
        public int successfulComponents() { return (int) items.stream().filter(FlushItem::successful).count(); }
    }
    public enum FailureKind { NOT_EXECUTED, UNKNOWN, CONFLICT, MISSING, INVALID }
    public record Failure(FailureKind kind, WriteCommand command, WriteResult.Status status, Throwable error) {}
    public record LocalView(long revision, Optional<Map<String, Object>> data) {}
    public record Stats(int components, int pendingOperations, long bufferedBytes, int blockedComponents,
                        boolean flushing, boolean closing, boolean closed) {}
    private static final class Entry {
        final Map<String, Object> replacement;
        final DocumentPatch patch;
        final String operationId = "dirty-" + UUID.randomUUID();
        boolean sealed;
        WriteCommand command;
        Snapshot after;
        final long reservedBytes;
        Entry(Map<String, Object> replacement, DocumentPatch patch, long imageBytes) {
            this.replacement = replacement; this.patch = patch;
            long payload = replacement != null ? DocumentData.estimatedBytes(replacement) :
                DocumentData.estimatedBytes(Map.of("set", patch.set(), "unset", patch.unset(), "inc", patch.increment()));
            reservedBytes = 128 + payload * 2 + imageBytes;
        }
        long bytes() { return reservedBytes; }
    }
    private static final class Component {
        final String name;
        final StoreKey key;
        final ArrayDeque<Entry> queue = new ArrayDeque<>();
        Snapshot confirmed;
        Map<String, Object> working;
        CompletableFuture<Optional<Snapshot>> loading;
        boolean opened;
        long revision;
        Failure failure;
        Component(String name, StoreKey key) { this.name = name; this.key = key; }
    }
    private record Batch(Component component, List<Entry> entries) {}
    private final DocumentStore store;
    private final String section, entityId;
    private final Limits limits;
    private final Runnable ownerCheck;
    private final Object lock = new Object();
    private final Map<String, Component> components = new LinkedHashMap<>();
    private CompletableFuture<FlushResult> activeFlush, closeFuture;
    private boolean closing, closed;

    public DirtyDocumentSet(DocumentStore store, String section, String entityId, int maxComponents, int maxPending) {
        this(store, section, entityId, maxComponents, maxPending, () -> {});
    }
    public DirtyDocumentSet(DocumentStore store, String section, String entityId,
                            int maxComponents, int maxPending, Runnable ownerCheck) {
        this(store, section, entityId, new Limits(maxComponents, maxPending, 8_388_608, 1_048_576, 1024), ownerCheck);
    }
    public DirtyDocumentSet(DocumentStore store, String section, String entityId, Limits limits, Runnable ownerCheck) {
        this.store = Objects.requireNonNull(store);
        new StoreKey(section, entityId);
        this.section = section; this.entityId = entityId;
        this.limits = Objects.requireNonNull(limits); this.ownerCheck = Objects.requireNonNull(ownerCheck);
    }
    /** Stable unambiguous key: the numeric entity-id length prefixes the component suffix. */
    public StoreKey key(String component) {
        validateName(component);
        return new StoreKey(section, entityId.length() + ":" + entityId + ":" + component);
    }
    public CompletionStage<Optional<Snapshot>> open(String name) {
        ownerCheck.run();
        Component c; CompletableFuture<Optional<Snapshot>> result;
        synchronized (lock) {
            requireMutable(); validateName(name);
            c = components.get(name);
            if (c == null) {
                if (components.size() >= limits.components()) throw new RejectedExecutionException("component capacity");
                c = new Component(name, key(name)); components.put(name, c);
            }
            if (c.opened) return CompletableFuture.completedFuture(Optional.ofNullable(c.confirmed));
            if (c.loading != null) return c.loading.minimalCompletionStage();
            result = new CompletableFuture<>(); c.loading = result;
        }
        Component component = c;
        CompletionStage<Optional<Snapshot>> load;
        try { load = Objects.requireNonNull(store.load(c.key)); }
        catch (Throwable e) { load = CompletableFuture.failedFuture(e); }
        load.whenComplete((snapshot, failure) -> {
            Throwable error = failure == null ? null : unwrap(failure);
            synchronized (lock) {
                if (error == null) try {
                    Objects.requireNonNull(snapshot);
                    Snapshot loaded = snapshot.orElse(null);
                    if (loaded != null) {
                        if (loaded.version() < 1) throw new IllegalArgumentException("loaded version must be positive");
                        checkDocument(loaded.data());
                        long extra = 2 * DocumentData.estimatedBytes(loaded.data());
                        if (bytes() + extra > limits.bufferedBytes()) throw new RejectedExecutionException("buffer bytes");
                    }
                    component.confirmed = loaded;
                    component.working = loaded == null ? null : loaded.data();
                    component.opened = true;
                } catch (Throwable e) { error = e; }
                component.loading = null;
            }
            if (error == null) result.complete(snapshot); else result.completeExceptionally(error);
        });
        return result.minimalCompletionStage();
    }
    public void markPatch(String name, DocumentPatch patch) {
        ownerCheck.run(); Objects.requireNonNull(patch);
        synchronized (lock) {
            requireMutable(); Component c = requireOpened(name); requireHealthy(c);
            if (c.working == null) throw new IllegalStateException("create missing component with a full snapshot first");
            if (pathCount(patch) > limits.patchPaths()) throw new RejectedExecutionException("patch paths");
            Map<String, Object> working = DocumentData.apply(c.working, patch);
            var queue = new ArrayDeque<>(c.queue);
            Entry tail = queue.peekLast();
            DocumentPatch merged = tail != null && !tail.sealed && tail.patch != null ? merge(tail.patch, patch) : null;
            if (merged != null && pathCount(merged) <= limits.patchPaths()) {
                queue.removeLast(); queue.addLast(new Entry(null, merged, DocumentData.estimatedBytes(working)));
            } else queue.addLast(new Entry(null, patch, DocumentData.estimatedBytes(working)));
            update(c, working, queue);
        }
    }
    /** Full authoritative local state; only unsealed intents can be superseded. */
    public void markReplace(String name, Map<String, ?> data) { markReplace(name, -1, data); }
    /** Supply a revision from localView to reject a stale business snapshot. */
    public void markReplace(String name, long expectedRevision, Map<String, ?> data) {
        ownerCheck.run(); Objects.requireNonNull(data);
        synchronized (lock) {
            requireMutable(); Component c = requireOpened(name); requireHealthy(c);
            if (expectedRevision < -1 || expectedRevision >= 0 && expectedRevision != c.revision)
                throw new IllegalStateException("stale local revision");
            checkDocument(data);
            Map<String, Object> frozen = Values.freeze(data);
            var queue = new ArrayDeque<>(c.queue);
            while (!queue.isEmpty() && !queue.peekLast().sealed) queue.removeLast();
            queue.addLast(new Entry(frozen, null, DocumentData.estimatedBytes(frozen)));
            update(c, frozen, queue);
        }
    }
    private void update(Component c, Map<String, Object> working, ArrayDeque<Entry> queue) {
        checkDocument(working);
        long revision = Math.addExact(c.revision, 1);
        int operations = operations() - c.queue.size() + queue.size();
        long total = bytes() - componentBytes(c) + componentBytes(c.confirmed, working, queue);
        if (operations > limits.operations() || total > limits.bufferedBytes())
            throw new RejectedExecutionException("dirty buffer capacity");
        c.working = working; c.queue.clear(); c.queue.addAll(queue); c.revision = revision;
    }
    public LocalView localView(String name) {
        ownerCheck.run();
        synchronized (lock) { var c = requireOpened(name); return new LocalView(c.revision, Optional.ofNullable(c.working)); }
    }
    /** Last confirmed database state; does not include pending local edits. */
    public Optional<Snapshot> current(String name) {
        ownerCheck.run();
        synchronized (lock) { return Optional.ofNullable(requireOpened(name).confirmed); }
    }
    public Optional<Failure> failure(String name) {
        synchronized (lock) { return Optional.ofNullable(requireOpened(name).failure); }
    }
    /** Explicitly retry only the exact pinned command after non-execution or uncertain acknowledgement. */
    public void retryFailed(String name) {
        ownerCheck.run();
        synchronized (lock) {
            requireNotClosed(); Component c = requireOpened(name);
            if (activeFlush != null) throw new IllegalStateException("flush still running");
            if (c.failure == null || c.failure.command() == null ||
                c.failure.kind() != FailureKind.NOT_EXECUTED && c.failure.kind() != FailureKind.UNKNOWN)
                throw new IllegalStateException("component requires application reconciliation");
            c.failure = null;
        }
    }
    public Set<String> dirtyComponents() {
        synchronized (lock) {
            Set<String> result = new LinkedHashSet<>();
            components.values().stream().filter(c -> !c.queue.isEmpty()).forEach(c -> result.add(c.name));
            return Set.copyOf(result);
        }
    }
    public int pendingOperations() { synchronized (lock) { return operations(); } }
    public Stats stats() {
        synchronized (lock) {
            return new Stats(components.size(), operations(), bytes(),
                (int) components.values().stream().filter(c -> c.failure != null).count(),
                activeFlush != null, closing, closed);
        }
    }
    /** Coalesces concurrent calls to the active boundary. Later edits need another flush. */
    public CompletionStage<FlushResult> flush() {
        ownerCheck.run(); return beginFlush().minimalCompletionStage();
    }
    private CompletableFuture<FlushResult> beginFlush() {
        List<Batch> batches; CompletableFuture<FlushResult> result;
        synchronized (lock) {
            requireNotClosed();
            if (activeFlush != null) return activeFlush;
            batches = new ArrayList<>();
            for (var c : components.values()) if (!c.queue.isEmpty()) {
                var entries = List.copyOf(c.queue);
                entries.forEach(e -> e.sealed = true);
                batches.add(new Batch(c, entries));
            }
            if (batches.isEmpty()) return CompletableFuture.completedFuture(new FlushResult(List.of()));
            result = new CompletableFuture<>(); activeFlush = result;
        }
        List<CompletableFuture<FlushItem>> futures = batches.stream().map(this::executeBatch).toList();
        CompletableFuture.allOf(futures.toArray(CompletableFuture<?>[]::new)).whenComplete((v, error) -> {
            var report = new FlushResult(futures.stream().map(CompletableFuture::join).toList());
            synchronized (lock) { activeFlush = null; }
            result.complete(report);
        });
        return result;
    }
    private CompletableFuture<FlushItem> executeBatch(Batch batch) {
        var c = batch.component;
        synchronized (lock) {
            if (c.failure != null) return CompletableFuture.completedFuture(
                new FlushItem(c.name, 0, 0, c.failure.status(), c.failure.error()));
        }
        CompletableFuture<FlushItem> chain = CompletableFuture.completedFuture(
            new FlushItem(c.name, 0, 0, WriteResult.Status.APPLIED, null));
        for (Entry entry : batch.entries) chain = chain.thenCompose(previous ->
            previous.successful() ? execute(c, entry, previous) : CompletableFuture.completedFuture(previous));
        return chain;
    }
    private CompletableFuture<FlushItem> execute(Component c, Entry entry, FlushItem previous) {
        synchronized (lock) {
            try {
                if (c.queue.peekFirst() != entry) throw new IllegalStateException("write ordering invariant");
                if (entry.command == null) {
                    long version = c.confirmed == null ? 0 : c.confirmed.version();
                    if (entry.replacement != null) {
                        entry.command = c.confirmed == null ? WriteCommand.create(entry.operationId, entry.replacement) :
                            WriteCommand.replace(version, entry.operationId, entry.replacement);
                    } else {
                        if (c.confirmed == null) throw new IllegalStateException("missing base document");
                        entry.command = WriteCommand.patch(version, entry.operationId, entry.patch);
                    }
                    Map<String, Object> data = entry.replacement != null ? entry.replacement :
                        DocumentData.apply(c.confirmed.data(), entry.patch);
                    entry.after = new Snapshot(Math.addExact(version, 1), data);
                }
                if (entry.after == null) throw new IllegalStateException("invalid prepared write");
            } catch (Throwable error) {
                c.failure = new Failure(FailureKind.INVALID, entry.command, null, error);
                return CompletableFuture.completedFuture(new FlushItem(c.name, previous.attempted(),
                    previous.applied(), null, error));
            }
        }
        CompletionStage<WriteResult> operation;
        try { operation = Objects.requireNonNull(store.write(c.key, entry.command)); }
        catch (Throwable error) { operation = CompletableFuture.failedFuture(error); }
        return operation.handle((write, error) -> {
            Throwable cause = error == null ? null : unwrap(error);
            synchronized (lock) {
                if (cause == null && (write == null || write.successful() && write.version() != entry.after.version()))
                    cause = new IllegalStateException("invalid driver result");
                if (cause != null) {
                    var kind = cause instanceof StorageException e && e.outcome() == StorageException.Outcome.NOT_EXECUTED ?
                        FailureKind.NOT_EXECUTED : FailureKind.UNKNOWN;
                    c.failure = new Failure(kind, entry.command, null, cause);
                } else if (!write.successful()) {
                    c.failure = new Failure(write.status() == WriteResult.Status.MISSING ? FailureKind.MISSING :
                        FailureKind.CONFLICT, entry.command, write.status(), null);
                } else {
                    c.confirmed = entry.after; c.queue.removeFirst();
                }
            }
            return new FlushItem(c.name, previous.attempted() + 1,
                previous.applied() + (cause == null && write.successful() ? 1 : 0),
                cause == null ? write.status() : null, cause);
        }).toCompletableFuture();
    }
    /** Stop accepting mutations now; drain the active boundary AND everything queued after it. */
    public CompletionStage<FlushResult> flushAndClose() {
        ownerCheck.run();
        CompletableFuture<FlushResult> result;
        synchronized (lock) {
            if (closed) return CompletableFuture.completedFuture(new FlushResult(List.of()));
            if (closeFuture != null) return closeFuture.minimalCompletionStage();
            if (components.values().stream().anyMatch(c -> c.loading != null))
                throw new IllegalStateException("wait for component loads before shutdown");
            closing = true; closeFuture = result = new CompletableFuture<>();
        }
        drainForClose(result, new LinkedHashMap<>());
        return result.minimalCompletionStage();
    }
    private void drainForClose(CompletableFuture<FlushResult> result, Map<String, FlushItem> reports) {
        beginFlush().whenComplete((report, error) -> {
            if (error != null) {
                synchronized (lock) { closeFuture = null; }
                result.completeExceptionally(unwrap(error)); return;
            }
            for (var item : report.items()) reports.merge(item.component(), item, (a, b) ->
                new FlushItem(b.component(), a.attempted() + b.attempted(), a.applied() + b.applied(), b.status(), b.error()));
            boolean drained;
            synchronized (lock) {
                drained = operations() == 0;
                if (report.complete() && drained) closed = true;
                if (!report.complete()) closeFuture = null; // explicit reconciliation/retry can attempt shutdown again
            }
            if (!report.complete() || drained) result.complete(new FlushResult(List.copyOf(reports.values())));
            else drainForClose(result, reports);
        });
    }
    /** No implicit I/O or data loss: dirty/in-flight buffers must use flushAndClose(). */
    @Override public void close() {
        ownerCheck.run();
        synchronized (lock) {
            if (closed) return;
            if (operations() != 0 || activeFlush != null || components.values().stream().anyMatch(c -> c.loading != null))
                throw new IllegalStateException("unflushed work; await flushAndClose before closing");
            closed = true;
        }
    }
    private Component requireOpened(String name) {
        Component c = components.get(name);
        if (c == null || !c.opened) throw new IllegalStateException("component is not opened: " + name);
        return c;
    }
    private void requireNotClosed() { if (closed) throw new RejectedExecutionException("dirty set closed"); }
    private void requireMutable() {
        requireNotClosed(); if (closing) throw new RejectedExecutionException("dirty set draining");
    }
    private static void requireHealthy(Component c) {
        if (c.failure != null) throw new IllegalStateException("component blocked: " + c.failure.kind());
    }
    private void checkDocument(Map<String, ?> data) {
        if (DocumentData.estimatedBytes(data) > limits.documentBytes())
            throw new RejectedExecutionException("document bytes");
    }
    private int operations() { return components.values().stream().mapToInt(c -> c.queue.size()).sum(); }
    private long bytes() { return components.values().stream().mapToLong(DirtyDocumentSet::componentBytes).sum(); }
    private static long componentBytes(Component c) { return componentBytes(c.confirmed, c.working, c.queue); }
    private static long componentBytes(Snapshot confirmed, Map<String, Object> working, Collection<Entry> queue) {
        return (confirmed == null ? 0 : DocumentData.estimatedBytes(confirmed.data())) +
            (working == null ? 0 : DocumentData.estimatedBytes(working)) + queue.stream().mapToLong(Entry::bytes).sum();
    }
    private static void validateName(String name) {
        if (name == null || !name.matches("[a-z][a-z0-9_]{0,31}")) throw new IllegalArgumentException("component name");
    }
    private static Throwable unwrap(Throwable e) {
        return (e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null ? e.getCause() : e;
    }
    private static int pathCount(DocumentPatch patch) { return patch.set().size() + patch.unset().size() + patch.increment().size(); }
    /** Combine validated scalar edits; preserve parent/child ordering and intermediate empty objects. */
    private static DocumentPatch merge(DocumentPatch a, DocumentPatch b) {
        Set<String> oldPaths = new HashSet<>(a.set().keySet()); oldPaths.addAll(a.unset()); oldPaths.addAll(a.increment().keySet());
        Set<String> newPaths = new HashSet<>(b.set().keySet()); newPaths.addAll(b.unset()); newPaths.addAll(b.increment().keySet());
        for (String x : oldPaths) for (String y : newPaths) {
            if (x.startsWith(y + ".") || y.startsWith(x + ".")) return null;
            // set/inc can create a parent object which a later unset leaves empty.
            if (x.equals(y) && x.contains(".") && b.unset().contains(y) &&
                (a.set().containsKey(x) || a.increment().containsKey(x))) return null;
        }
        var set = new HashMap<>(a.set()); var unset = new HashSet<>(a.unset()); var inc = new HashMap<>(a.increment());
        b.set().forEach((k, v) -> { set.put(k, v); unset.remove(k); inc.remove(k); });
        b.unset().forEach(k -> { set.remove(k); inc.remove(k); unset.add(k); });
        try {
            for (var entry : b.increment().entrySet()) {
                String key = entry.getKey(); long delta = entry.getValue();
                if (set.containsKey(key)) set.put(key, Math.addExact(((Number) set.get(key)).longValue(), delta));
                else if (unset.remove(key)) set.put(key, delta);
                else inc.put(key, Math.addExact(inc.getOrDefault(key, 0L), delta));
            }
        } catch (ArithmeticException error) {
            // Delta sum can overflow even when both sequential values are representable.
            return null;
        }
        return new DocumentPatch(set, unset, inc);
    }
}
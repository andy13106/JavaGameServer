package io.gameframe.transport;

import io.gameframe.runtime.RpcCallExecutor;
import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceDirectoryView;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

/**
 * Routes RPC attempts through a remote ServiceDirectoryView.
 *
 * <p>Each retry performs a fresh service selection. A retryable failure
 * temporarily excludes the failed instance, allowing Gate/Game calls to move
 * to another live incarnation while preserving the same command ID.</p>
 */
public final class ServiceDirectoryRpcRouter implements AutoCloseable {
    @FunctionalInterface
    public interface Invoker<P, R> {
        CompletionStage<R> invoke(ServiceDirectory.ServiceInstance target,
                                  GameRpcClient.Request<P> request);
    }

    public static final class NoServiceException extends RuntimeException {
        public NoServiceException(String role) {
            super("no accepting service instance for role: " + role);
        }
    }

    private final ServiceDirectoryView directory;
    private final RpcCallExecutor executor;
    private final long unavailableMillis;
    private final ConcurrentHashMap<String, Long> unavailableUntil = new ConcurrentHashMap<>();

    public ServiceDirectoryRpcRouter(ServiceDirectoryView directory) {
        this(directory, new RpcCallExecutor(), Duration.ofSeconds(1));
    }

    public ServiceDirectoryRpcRouter(ServiceDirectoryView directory,
                                     RpcCallExecutor executor,
                                     Duration unavailableDuration) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.executor = Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(unavailableDuration, "unavailableDuration");
        if (unavailableDuration.isNegative() || unavailableDuration.isZero()) {
            throw new IllegalArgumentException("unavailableDuration must be positive");
        }
        unavailableMillis = unavailableDuration.toMillis();
    }

    public <P, R> CompletionStage<R> call(String role,
                                       Set<String> requiredCapabilities,
                                       String routingKey,
                                       String commandId,
                                       P payload,
                                       long deadlineMillis,
                                       RpcCallExecutor.RetryMode retryMode,
                                       Predicate<Throwable> retryable,
                                       Invoker<P, R> invoker) {
        requireText(role, "role");
        requireText(routingKey, "routingKey");
        requireText(commandId, "commandId");
        Objects.requireNonNull(requiredCapabilities, "requiredCapabilities");
        Objects.requireNonNull(retryMode, "retryMode");
        Objects.requireNonNull(retryable, "retryable");
        Objects.requireNonNull(invoker, "invoker");
        if (deadlineMillis <= System.currentTimeMillis()) {
            throw new IllegalArgumentException("deadline must be in the future");
        }

        var lastTarget = new AtomicReference<String>();
        return executor.execute(role, commandId, retryMode, deadlineMillis, () -> {
            long now = System.currentTimeMillis();
            Set<String> excluded = unavailable(now);
            var target = directory.chooseExcluding(role, requiredCapabilities, routingKey, excluded, now)
                    .orElseThrow(() -> new NoServiceException(role));
            lastTarget.set(target.instanceId());
            var request = new GameRpcClient.Request<>(target.endpoint().toString(), commandId, payload, deadlineMillis);
            return invoker.invoke(target, request);
        }, error -> {
            boolean retry = retryable.test(error);
            if (retry) {
                String failed = lastTarget.get();
                if (failed != null) unavailableUntil.put(failed, safeAdd(System.currentTimeMillis(), unavailableMillis));
            }
            return retry;
        });
    }

    public RpcCallExecutor.Stats stats() { return executor.stats(); }

    private Set<String> unavailable(long nowMillis) {
        var excluded = new HashSet<String>();
        unavailableUntil.forEach((instanceId, until) -> {
            if (until > nowMillis) excluded.add(instanceId);
            else unavailableUntil.remove(instanceId, until);
        });
        return Set.copyOf(excluded);
    }

    private static long safeAdd(long nowMillis, long durationMillis) {
        return Long.MAX_VALUE - nowMillis < durationMillis ? Long.MAX_VALUE : nowMillis + durationMillis;
    }

    @Override public void close() {
        unavailableUntil.clear();
        executor.close();
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " required");
    }
}

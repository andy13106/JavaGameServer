package io.gameframe.transport;

import io.gameframe.runtime.RpcCallExecutor;
import io.gameframe.runtime.ServiceDirectory;
import io.gameframe.runtime.ServiceDirectoryView;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ServiceDirectoryRpcRouterTest {
    @Test
    void selectsRemoteTargetAndPreservesCommandIdAndDeadline() throws Exception {
        var view = new ServiceDirectoryView();
        long now = System.currentTimeMillis();
        view.upsert(registration("game", "one", 1, "http://127.0.0.1:19001"),
                new ServiceDirectory.Lease("game", "one", 1, 11), now + 60_000, 0, false, now);
        try (var router = new ServiceDirectoryRpcRouter(view)) {
            var command = "command-1";
            var seen = new AtomicReference<GameRpcClient.Request<String>>();
            var result = router.call("game", Set.of("rpc"), "player-1", command, "payload",
                    System.currentTimeMillis() + 2_000, RpcCallExecutor.RetryMode.NEVER,
                    error -> false, (target, request) -> {
                        seen.set(request);
                        return CompletableFuture.completedFuture("ok");
                    }).toCompletableFuture().get(1, TimeUnit.SECONDS);

            assertEquals("ok", result);
            assertEquals(command, seen.get().commandId());
            assertEquals("http://127.0.0.1:19001", seen.get().destination());
        }
    }

    @Test
    void retryableFailureExcludesFailedInstanceAndSelectsAnother() throws Exception {
        long now = System.currentTimeMillis();
        var view = new ServiceDirectoryView();
        view.upsert(registration("game", "one", 1, "http://127.0.0.1:19001"),
                new ServiceDirectory.Lease("game", "one", 1, 11), now + 60_000, 0, false, now);
        view.upsert(registration("game", "two", 1, "http://127.0.0.1:19002"),
                new ServiceDirectory.Lease("game", "two", 1, 22), now + 60_000, 0, false, now);
        try (var router = new ServiceDirectoryRpcRouter(view, new RpcCallExecutor(
                new RpcCallExecutor.Policy(2, Duration.ofMillis(500), Duration.ZERO,
                        Duration.ZERO, 5, Duration.ofSeconds(1), 8, 64), 1, "router-test"),
                Duration.ofSeconds(1))) {
            var attempts = new AtomicInteger();
            var destinations = new java.util.ArrayList<String>();
            var result = router.call("game", Set.of("rpc"), "same-key", "command-2", "payload",
                    System.currentTimeMillis() + 2_000, RpcCallExecutor.RetryMode.IDEMPOTENT,
                    error -> true, (target, request) -> {
                        attempts.incrementAndGet();
                        destinations.add(request.destination());
                        if (attempts.get() == 1) return CompletableFuture.failedFuture(new RuntimeException("down"));
                        return CompletableFuture.completedFuture("recovered");
                    }).toCompletableFuture().get(1, TimeUnit.SECONDS);

            assertEquals("recovered", result);
            assertEquals(2, attempts.get());
            assertNotEquals(destinations.get(0), destinations.get(1));
        }
    }

    @Test
    void reportsMissingRoleWithoutInvokingTransport() {
        var view = new ServiceDirectoryView();
        try (var router = new ServiceDirectoryRpcRouter(view)) {
            var stage = router.call("missing", Set.of(), "key", "command-3", "payload",
                    System.currentTimeMillis() + 1_000, RpcCallExecutor.RetryMode.NEVER,
                    error -> false, (target, request) -> {
                        fail("invoker must not run");
                        return CompletableFuture.completedFuture("bad");
                    });
            assertThrows(Exception.class, () -> stage.toCompletableFuture().get(1, TimeUnit.SECONDS));
        }
    }

    private static ServiceDirectory.Registration registration(String role, String id, long generation,
                                                               String endpoint) {
        return new ServiceDirectory.Registration(role, id, URI.create(endpoint), Set.of("rpc"), 1, 60_000, generation);
    }
}

package io.gameframe.transport;

import com.zfoo.net.session.Session;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class GameSessionRouterTest {
    @Test
    void routesWorkSeriallyAndDisconnectStopsFurtherDelivery() throws Exception {
        try (var router = new GameSessionRouter(2, 4, 8, 1)) {
            var channel = new EmbeddedChannel();
            try {
                Session session = new Session(101, 7, channel);
                router.bind(session, "player-7");
                var order = new CopyOnWriteArrayList<Integer>();
                CompletionStage<Integer> first = router.dispatch(session, () -> {
                    order.add(1); Thread.sleep(20); return 11;
                });
                CompletionStage<Integer> second = router.dispatch(session, () -> {
                    order.add(2); return 22;
                });
                assertEquals(11, first.toCompletableFuture().get(2, TimeUnit.SECONDS));
                assertEquals(22, second.toCompletableFuture().get(2, TimeUnit.SECONDS));
                assertEquals(java.util.List.of(1, 2), order);
                assertEquals(1, router.boundCount());

                assertDoesNotThrow(() -> router.disconnect(session).toCompletableFuture().get(2, TimeUnit.SECONDS));
                assertEquals(0, router.boundCount());
                assertThrows(ExecutionException.class,
                    () -> router.dispatch(session, () -> 1).toCompletableFuture().get(2, TimeUnit.SECONDS));
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void oldDisconnectCannotRemoveReplacementActorBinding() throws Exception {
        try (var router = new GameSessionRouter(2, 4, 4, 1)) {
            var oldChannel = new EmbeddedChannel();
            var newChannel = new EmbeddedChannel();
            try {
                Session oldSession = new Session(201, 9, oldChannel);
                router.bind(oldSession, "player-9");
                router.disconnect(oldSession).toCompletableFuture().get(2, TimeUnit.SECONDS);

                Session newSession = new Session(202, 9, newChannel);
                var replacement = router.bind(newSession, "player-9");
                assertEquals(2, replacement.generation());
                assertEquals(1, router.boundCount());

                router.disconnect(oldSession).toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertTrue(router.binding(newSession).isPresent());
                assertEquals(9, router.dispatch(newSession, newSession::getUid)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS));
            } finally {
                oldChannel.finishAndReleaseAll();
                newChannel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void duplicateSessionAndActorBindingsAreRejected() {
        try (var router = new GameSessionRouter(1, 2, 2, 1)) {
            var firstChannel = new EmbeddedChannel();
            var secondChannel = new EmbeddedChannel();
            try {
                Session first = new Session(301, 1, firstChannel);
                Session second = new Session(302, 1, secondChannel);
                router.bind(first, "player-1");
                assertThrows(IllegalStateException.class, () -> router.bind(first, "other"));
                assertThrows(IllegalStateException.class, () -> router.bind(second, "player-1"));
            } finally {
                firstChannel.finishAndReleaseAll();
                secondChannel.finishAndReleaseAll();
            }
        }
    }
}


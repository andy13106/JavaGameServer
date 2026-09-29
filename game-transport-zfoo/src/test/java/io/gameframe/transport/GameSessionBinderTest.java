package io.gameframe.transport;

import com.zfoo.net.session.Session;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class GameSessionBinderTest {
    @Test
    void loginMovesTemporarySidActorToUidActorAndLogoutStopsIt() throws Exception {
        try (var sessions = new GameSessionRouter(1, 8, 8, 1)) {
            var binder = new GameSessionBinder(sessions);
            var channel = new EmbeddedChannel();
            try {
                Session session = new Session(601, 0, channel);
                sessions.bind(session, "sid:601");

                var loggedIn = binder.login(session, 1001)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertEquals("uid:1001", loggedIn.actorId());
                assertEquals(1001, session.getUid());
                assertEquals(1, sessions.boundCount());
                assertSame(loggedIn, sessions.binding(session).orElseThrow());

                binder.login(session, 1001).toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertEquals(1, sessions.boundCount());

                binder.logout(session).toCompletableFuture().get(2, TimeUnit.SECONDS);
                assertEquals(0, sessions.boundCount());
                assertThrows(ExecutionException.class, () -> sessions.dispatch(session, () -> 1)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS));
            } finally {
                channel.finishAndReleaseAll();
            }
        }
    }

    @Test
    void duplicateUidDoesNotDisplaceExistingSession() throws Exception {
        try (var sessions = new GameSessionRouter(1, 8, 8, 1)) {
            var binder = new GameSessionBinder(sessions);
            var firstChannel = new EmbeddedChannel();
            var secondChannel = new EmbeddedChannel();
            try {
                Session first = new Session(602, 0, firstChannel);
                Session second = new Session(603, 0, secondChannel);
                binder.login(first, 2002).toCompletableFuture().get(2, TimeUnit.SECONDS);

                assertThrows(ExecutionException.class, () -> binder.login(second, 2002)
                    .toCompletableFuture().get(2, TimeUnit.SECONDS));
                assertEquals(0, second.getUid());
                assertTrue(sessions.binding(first).isPresent());
                assertTrue(sessions.binding(second).isEmpty());
            } finally {
                firstChannel.finishAndReleaseAll();
                secondChannel.finishAndReleaseAll();
            }
        }
    }
}


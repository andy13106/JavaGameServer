package io.gameframe.transport;

import com.zfoo.net.session.Session;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

class GameAuthenticationServiceTest {
    @Test
    void validCredentialsConsumeNonceAndPromoteSessionToUidActor() throws Exception {
        var clock = new AtomicLong(1_000);
        var replay = new ReplayGuard(new ReplayGuard.Limits(
                16, Duration.ofSeconds(10), Duration.ZERO, Duration.ofSeconds(20)), clock::get);
        try (var sessions = new GameSessionRouter(1, 4, 8, 1)) {
            var service = service(sessions, replay, new byte[]{1, 2, 3});
            var session = new Session(701, 0, new EmbeddedChannel());

            var result = service.authenticate(session,
                    new GameAuthenticationService.Credentials("account-7", 1_000, "nonce-1", new byte[]{1, 2, 3}))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);

            assertTrue(result.accepted());
            assertNull(result.rejection());
            assertEquals(7, result.binding().session().getUid());
            assertEquals("uid:7", result.binding().actorId());
            assertEquals(1, sessions.boundCount());
        }
    }

    @Test
    void invalidSecretDoesNotConsumeNonceAndReplayIsRejectedAfterSuccess() throws Exception {
        var clock = new AtomicLong(2_000);
        var replay = new ReplayGuard(new ReplayGuard.Limits(
                16, Duration.ofSeconds(10), Duration.ZERO, Duration.ofSeconds(20)), clock::get);
        try (var sessions = new GameSessionRouter(1, 4, 8, 1)) {
            var service = service(sessions, replay, new byte[]{9, 8, 7});
            var session = new Session(702, 0, new EmbeddedChannel());
            var invalid = new GameAuthenticationService.Credentials("account-7", 2_000, "nonce-2", new byte[]{0});

            assertEquals(GameAuthenticationService.Rejection.INVALID_CREDENTIALS,
                    service.authenticate(session, invalid).toCompletableFuture().join().rejection());
            var valid = new GameAuthenticationService.Credentials("account-7", 2_000, "nonce-2", new byte[]{9, 8, 7});
            assertTrue(service.authenticate(session, valid).toCompletableFuture().get(2, TimeUnit.SECONDS).accepted());
            assertEquals(GameAuthenticationService.Rejection.REPLAY,
                    service.authenticate(session, valid).toCompletableFuture().join().rejection());
        }
    }

    @Test
    void duplicateUidKeepsOriginalSessionBound() throws Exception {
        var clock = new AtomicLong(3_000);
        var replay = new ReplayGuard(new ReplayGuard.Limits(
                16, Duration.ofSeconds(10), Duration.ZERO, Duration.ofSeconds(20)), clock::get);
        try (var sessions = new GameSessionRouter(1, 4, 8, 1)) {
            var service = service(sessions, replay, new byte[]{4});
            var first = new Session(703, 0, new EmbeddedChannel());
            var second = new Session(704, 0, new EmbeddedChannel());
            var firstResult = service.authenticate(first,
                    new GameAuthenticationService.Credentials("account-7", 3_000, "nonce-3", new byte[]{4}))
                    .toCompletableFuture().get(2, TimeUnit.SECONDS);
            var secondResult = service.authenticate(second,
                    new GameAuthenticationService.Credentials("account-7", 3_000, "nonce-4", new byte[]{4}))
                    .toCompletableFuture().join();

            assertTrue(firstResult.accepted());
            assertEquals(GameAuthenticationService.Rejection.SESSION_BINDING, secondResult.rejection());
            assertSame(first, sessions.bindingByActor("uid:7").orElseThrow().session());
        }
    }

    private static GameAuthenticationService service(GameSessionRouter sessions,
                                                    ReplayGuard replay,
                                                    byte[] secret) {
        var binder = new GameSessionBinder(sessions);
        return new GameAuthenticationService(binder, replay,
                identity -> "account-7".equals(identity)
                        ? Optional.of(new GameAuthenticationService.Account(7, secret))
                        : Optional.empty());
    }
}
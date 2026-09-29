package io.gameframe.transport;

import com.zfoo.net.session.Session;

import java.security.MessageDigest;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * Authenticates a transport session before it is promoted to a uid actor.
 *
 * <p>The service deliberately keeps credential lookup outside the framework so
 * MongoDB, MySQL, Redis, or a remote account service can provide the same
 * account contract. Secret comparison is constant-time and a valid request
 * consumes a nonce before the asynchronous session bind begins.</p>
 */
public final class GameAuthenticationService {
    public enum Rejection {
        INVALID_CREDENTIALS,
        REPLAY,
        SESSION_BINDING
    }

    public record Credentials(String identity, long timestampMillis, String nonce, byte[] secret) {
        public Credentials {
            if (identity == null || identity.isBlank()) {
                throw new IllegalArgumentException("identity required");
            }
            if (nonce == null || nonce.isBlank()) {
                throw new IllegalArgumentException("nonce required");
            }
            Objects.requireNonNull(secret, "secret");
            secret = secret.clone();
        }

        @Override
        public byte[] secret() {
            return secret.clone();
        }
    }

    public record Account(long uid, byte[] secret) {
        public Account {
            if (uid <= 0) {
                throw new IllegalArgumentException("uid must be positive");
            }
            Objects.requireNonNull(secret, "secret");
            secret = secret.clone();
        }

        @Override
        public byte[] secret() {
            return secret.clone();
        }
    }

    public record Result(boolean accepted,
                         Rejection rejection,
                         GameSessionRouter.Binding binding) {
        public Result {
            if (accepted && (rejection != null || binding == null)) {
                throw new IllegalArgumentException("accepted result requires binding and no rejection");
            }
            if (!accepted && (rejection == null || binding != null)) {
                throw new IllegalArgumentException("rejected result requires rejection and no binding");
            }
        }

        static Result accepted(GameSessionRouter.Binding binding) {
            return new Result(true, null, binding);
        }

        static Result rejected(Rejection rejection) {
            return new Result(false, rejection, null);
        }
    }

    private final GameSessionBinder binder;
    private final ReplayGuard replayGuard;
    private final Function<String, Optional<Account>> accountLookup;

    public GameAuthenticationService(GameSessionBinder binder,
                                     ReplayGuard replayGuard,
                                     Function<String, Optional<Account>> accountLookup) {
        this.binder = Objects.requireNonNull(binder, "binder");
        this.replayGuard = Objects.requireNonNull(replayGuard, "replayGuard");
        this.accountLookup = Objects.requireNonNull(accountLookup, "accountLookup");
    }

    /**
     * Authenticates and promotes a session. Invalid credentials and replayed
     * requests complete normally with a rejection so callers can rate-limit or
     * close the transport without using exceptional control flow.
     */
    public CompletionStage<Result> authenticate(Session session, Credentials credentials) {
        Objects.requireNonNull(session, "session");
        Objects.requireNonNull(credentials, "credentials");

        Optional<Account> account = accountLookup.apply(credentials.identity());
        if (account.isEmpty() || !constantTimeEquals(account.get().secret(), credentials.secret())) {
            return CompletableFuture.completedFuture(Result.rejected(Rejection.INVALID_CREDENTIALS));
        }

        ReplayGuard.Decision replay = replayGuard.accept(
                credentials.identity(), credentials.timestampMillis(), credentials.nonce());
        if (!replay.accepted()) {
            return CompletableFuture.completedFuture(Result.rejected(Rejection.REPLAY));
        }

        return binder.login(session, account.get().uid())
                .thenApply(Result::accepted)
                .exceptionally(ignored -> Result.rejected(Rejection.SESSION_BINDING));
    }

    private static boolean constantTimeEquals(byte[] expected, byte[] presented) {
        return MessageDigest.isEqual(expected, presented);
    }
}
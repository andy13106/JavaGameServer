package io.gameframe.transport;

import com.zfoo.net.session.Session;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;

/**
 * Performs the authenticated transition from a transport session actor to a uid actor.
 *
 * <p>The transition first removes the temporary sid binding, waits for its actor
 * to stop, then installs the uid binding. During that gap packets are rejected.
 * A uid already owned by another live session is rejected without changing the
 * caller's current binding.</p>
 */
public final class GameSessionBinder {
    private final GameSessionRouter sessions;

    public GameSessionBinder(GameSessionRouter sessions) {
        this.sessions = Objects.requireNonNull(sessions);
    }

    public CompletionStage<GameSessionRouter.Binding> login(Session session, long uid) {
        Objects.requireNonNull(session);
        if (uid <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("uid must be positive"));
        }

        String actorId = actorId(uid);
        var current = sessions.binding(session);
        if (current.isPresent() && current.get().actorId().equals(actorId)) {
            session.setUid(uid);
            return CompletableFuture.completedFuture(current.get());
        }

        var owner = sessions.bindingByActor(actorId);
        if (owner.isPresent() && owner.get().session() != session) {
            return CompletableFuture.failedFuture(
                new RejectedExecutionException("uid already owns a live session: " + uid));
        }

        CompletionStage<Void> released = current.isPresent()
            ? sessions.disconnect(session)
            : CompletableFuture.completedFuture(null);
        return released.thenApply(ignored -> {
            session.setUid(uid);
            return sessions.bind(session, actorId);
        });
    }

    public CompletionStage<Void> logout(Session session) {
        Objects.requireNonNull(session);
        session.setUid(0);
        return sessions.disconnect(session);
    }

    public static String actorId(long uid) {
        if (uid <= 0) throw new IllegalArgumentException("uid must be positive");
        return "uid:" + uid;
    }
}


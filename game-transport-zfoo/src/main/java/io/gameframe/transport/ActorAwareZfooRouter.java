package io.gameframe.transport;

import com.zfoo.net.router.Router;
import com.zfoo.net.task.PacketReceiverTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;

/**
 * zfoo Router variant that invokes packet receivers inside the bound GameFrame actor.
 *
 * <p>Unbound sessions keep zfoo's original task routing. Once a session is bound,
 * every normal request, gateway request and HTTP attachment dispatched through
 * {@code dispatchBySession} or {@code dispatchByTaskExecutorHash} enters the same
 * actor mailbox. The receiver's {@code @PacketReceiver Task} value is therefore
 * intentionally ignored for bound game sessions.</p>
 */
public final class ActorAwareZfooRouter extends Router {
    private static final Logger logger = LoggerFactory.getLogger(ActorAwareZfooRouter.class);

    private final GameSessionRouter sessions;
    private final BiConsumer<PacketReceiverTask, Throwable> dispatchFailureHandler;

    public ActorAwareZfooRouter(GameSessionRouter sessions) {
        this(sessions, (task, error) -> logger.warn(
            "drop packet [{}] for stale or unavailable game session sid=[{}] uid=[{}]",
            task.getPacket().getClass().getSimpleName(),
            task.getSession().getSid(),
            task.getSession().getUid(),
            error));
    }

    public ActorAwareZfooRouter(GameSessionRouter sessions,
                                BiConsumer<PacketReceiverTask, Throwable> dispatchFailureHandler) {
        this.sessions = Objects.requireNonNull(sessions);
        this.dispatchFailureHandler = Objects.requireNonNull(dispatchFailureHandler);
    }

    @Override
    public void dispatchBySession(PacketReceiverTask task) {
        if (!dispatchToBoundActor(task)) {
            super.dispatchBySession(task);
        }
    }

    @Override
    public void dispatchByTaskExecutorHash(int taskExecutorHash, PacketReceiverTask task) {
        if (!dispatchToBoundActor(task)) {
            super.dispatchByTaskExecutorHash(taskExecutorHash, task);
        }
    }

    private boolean dispatchToBoundActor(PacketReceiverTask task) {
        if (!sessions.isManaged(task.getSession())) {
            return false;
        }
        if (sessions.binding(task.getSession()).isEmpty()) {
            dispatchFailureHandler.accept(task, new RejectedExecutionException("session is retired"));
            return true;
        }
        sessions.dispatch(task.getSession(), () -> {
            super.atReceiver(task);
            return null;
        }).whenComplete((ignored, error) -> {
            if (error != null) {
                dispatchFailureHandler.accept(task, unwrap(error));
            }
        });
        return true;
    }

    private static Throwable unwrap(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null
            ? error.getCause()
            : error;
    }
}


package io.gameframe.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class OutboundPacketQueue {
    private static final AtomicLong IDS = new AtomicLong();

    public record Limits(int maxSessionPackets, long maxSessionBytes, long maxGlobalBytes) {
        public Limits {
            if (maxSessionPackets < 1 || maxSessionBytes < 1 || maxGlobalBytes < 1) {
                throw new IllegalArgumentException("outbound limits must be positive");
            }
            if (maxSessionBytes > maxGlobalBytes) {
                throw new IllegalArgumentException("session byte limit must not exceed global byte limit");
            }
        }
    }

    public enum SendOutcome {
        SENT,
        SUPERSEDED
    }

    public record Snapshot(long pendingPackets,
                           long pendingBytes,
                           long globalPendingBytes,
                           long accepted,
                           long sent,
                           long coalesced,
                           long rejectedPackets,
                           long rejectedBytes,
                           long failedWrites) {
    }

    private final Limits limits;
    private final AtomicLong globalPendingBytes = new AtomicLong();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder sent = new LongAdder();
    private final LongAdder coalesced = new LongAdder();
    private final LongAdder rejectedPackets = new LongAdder();
    private final LongAdder rejectedBytes = new LongAdder();
    private final LongAdder failedWrites = new LongAdder();
    private final AttributeKey<State> stateKey;
    private final String handlerName;

    public OutboundPacketQueue(Limits limits) {
        this.limits = Objects.requireNonNull(limits, "limits");
        var id = IDS.incrementAndGet();
        this.stateKey = AttributeKey.valueOf("gameframe.outboundQueue." + id);
        this.handlerName = "gameOutboundQueue." + id;
    }

    public CompletionStage<SendOutcome> send(Channel channel, Object message, int encodedBytes) {
        return enqueue(channel, message, encodedBytes, null);
    }

    public CompletionStage<SendOutcome> sendLatest(Channel channel,
                                                    Object coalesceKey,
                                                    Object message,
                                                    int encodedBytes) {
        return enqueue(channel, message, encodedBytes, Objects.requireNonNull(coalesceKey, "coalesceKey"));
    }

    public Snapshot snapshot(Channel channel) {
        var state = channel.attr(stateKey).get();
        return new Snapshot(
                state == null ? 0 : state.pendingPackets,
                state == null ? 0 : state.pendingBytes,
                globalPendingBytes.get(),
                accepted.sum(),
                sent.sum(),
                coalesced.sum(),
                rejectedPackets.sum(),
                rejectedBytes.sum(),
                failedWrites.sum());
    }

    public long globalPendingBytes() {
        return globalPendingBytes.get();
    }

    private CompletionStage<SendOutcome> enqueue(Channel channel,
                                                  Object message,
                                                  int encodedBytes,
                                                  Object coalesceKey) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(message, "message");
        if (encodedBytes < 1) {
            ReferenceCountUtil.release(message);
            return CompletableFuture.failedFuture(new IllegalArgumentException("encodedBytes must be positive"));
        }

        var result = new CompletableFuture<SendOutcome>();
        try {
            channel.eventLoop().execute(() -> enqueueOnEventLoop(
                    channel, message, encodedBytes, coalesceKey, result));
        } catch (RejectedExecutionException error) {
            ReferenceCountUtil.release(message);
            result.completeExceptionally(error);
        }
        return result.minimalCompletionStage();
    }

    private void enqueueOnEventLoop(Channel channel,
                                    Object message,
                                    int encodedBytes,
                                    Object coalesceKey,
                                    CompletableFuture<SendOutcome> result) {
        if (!channel.isActive()) {
            ReferenceCountUtil.release(message);
            result.completeExceptionally(new RejectedExecutionException("channel is closed"));
            return;
        }

        var state = state(channel);
        if (coalesceKey != null) {
            var existing = state.latestByKey.get(coalesceKey);
            if (existing != null) {
                replaceQueued(state, existing, message, encodedBytes, result);
                scheduleDrain(channel, state);
                return;
            }
        }

        if (state.pendingPackets >= limits.maxSessionPackets()) {
            rejectedPackets.increment();
            reject(message, result, "session outbound packet limit");
            return;
        }
        if (encodedBytes > limits.maxSessionBytes() - state.pendingBytes) {
            rejectedBytes.increment();
            reject(message, result, "session outbound byte limit");
            return;
        }
        if (!tryReserveGlobal(encodedBytes)) {
            rejectedBytes.increment();
            reject(message, result, "global outbound byte limit");
            return;
        }

        var entry = new Entry(message, encodedBytes, coalesceKey, result);
        state.queue.addLast(entry);
        if (coalesceKey != null) {
            state.latestByKey.put(coalesceKey, entry);
        }
        state.pendingPackets++;
        state.pendingBytes += encodedBytes;
        accepted.increment();
        scheduleDrain(channel, state);
    }

    private void replaceQueued(State state,
                               Entry existing,
                               Object message,
                               int encodedBytes,
                               CompletableFuture<SendOutcome> result) {
        var delta = (long) encodedBytes - existing.encodedBytes;
        if (delta > 0 && (delta > limits.maxSessionBytes() - state.pendingBytes || !tryReserveGlobal(delta))) {
            rejectedBytes.increment();
            reject(message, result, "outbound byte limit while coalescing");
            return;
        }
        if (delta < 0) {
            releaseGlobal(-delta);
        }

        var oldMessage = existing.message;
        var oldResult = existing.result;
        existing.message = message;
        existing.encodedBytes = encodedBytes;
        existing.result = result;
        state.pendingBytes += delta;
        accepted.increment();
        coalesced.increment();
        ReferenceCountUtil.release(oldMessage);
        oldResult.complete(SendOutcome.SUPERSEDED);
    }

    private void scheduleDrain(Channel channel, State state) {
        if (state.drainScheduled || state.writing) {
            return;
        }
        state.drainScheduled = true;
        channel.eventLoop().execute(() -> {
            state.drainScheduled = false;
            drain(channel, state);
        });
    }

    private void drain(Channel channel, State state) {
        if (state.writing || state.queue.isEmpty() || !channel.isActive() || !channel.isWritable()) {
            return;
        }

        var entry = state.queue.removeFirst();
        if (entry.coalesceKey != null) {
            state.latestByKey.remove(entry.coalesceKey, entry);
        }
        state.writing = true;
        state.current = entry;
        try {
            channel.writeAndFlush(entry.message).addListener(future ->
                    finishWrite(channel, state, entry, future.isSuccess(), future.cause()));
        } catch (Throwable error) {
            ReferenceCountUtil.release(entry.message);
            finishWrite(channel, state, entry, false, error);
        }
    }

    private void finishWrite(Channel channel,
                             State state,
                             Entry entry,
                             boolean success,
                             Throwable error) {
        if (state.current != entry) {
            return;
        }
        state.current = null;
        state.writing = false;
        releaseEntryBudget(state, entry);
        if (success) {
            sent.increment();
            entry.result.complete(SendOutcome.SENT);
            scheduleDrain(channel, state);
        } else {
            failedWrites.increment();
            entry.result.completeExceptionally(error == null
                    ? new IllegalStateException("outbound write failed")
                    : error);
            failQueued(state, new RejectedExecutionException("outbound channel write failed"));
        }
    }

    private State state(Channel channel) {
        var state = channel.attr(stateKey).get();
        if (state == null) {
            var created = new State();
            var prior = channel.attr(stateKey).setIfAbsent(created);
            state = prior == null ? created : prior;
        }
        if (channel.pipeline().get(handlerName) == null) {
            channel.pipeline().addLast(handlerName, new QueueLifecycleHandler(this));
        }
        return state;
    }

    private void onWritable(Channel channel) {
        var state = channel.attr(stateKey).get();
        if (state != null) {
            scheduleDrain(channel, state);
        }
    }

    private void onInactive(Channel channel) {
        var state = channel.attr(stateKey).get();
        if (state != null) {
            failQueued(state, new RejectedExecutionException("channel closed with queued packets"));
        }
    }

    private void failQueued(State state, Throwable error) {
        Entry entry;
        while ((entry = state.queue.pollFirst()) != null) {
            if (entry.coalesceKey != null) {
                state.latestByKey.remove(entry.coalesceKey, entry);
            }
            ReferenceCountUtil.release(entry.message);
            releaseEntryBudget(state, entry);
            entry.result.completeExceptionally(error);
        }
        state.drainScheduled = false;
    }

    private void releaseEntryBudget(State state, Entry entry) {
        state.pendingPackets--;
        state.pendingBytes -= entry.encodedBytes;
        releaseGlobal(entry.encodedBytes);
    }

    private boolean tryReserveGlobal(long bytes) {
        long pending = globalPendingBytes.get();
        while (true) {
            if (bytes > limits.maxGlobalBytes() - pending) {
                return false;
            }
            if (globalPendingBytes.compareAndSet(pending, pending + bytes)) {
                return true;
            }
            pending = globalPendingBytes.get();
        }
    }

    private void releaseGlobal(long bytes) {
        globalPendingBytes.addAndGet(-bytes);
    }

    private static void reject(Object message,
                               CompletableFuture<SendOutcome> result,
                               String reason) {
        ReferenceCountUtil.release(message);
        result.completeExceptionally(new RejectedExecutionException(reason));
    }

    private static final class State {
        private final ArrayDeque<Entry> queue = new ArrayDeque<>();
        private final Map<Object, Entry> latestByKey = new HashMap<>();
        private volatile long pendingPackets;
        private volatile long pendingBytes;
        private boolean writing;
        private boolean drainScheduled;
        private Entry current;
    }

    private static final class Entry {
        private Object message;
        private int encodedBytes;
        private final Object coalesceKey;
        private CompletableFuture<SendOutcome> result;

        private Entry(Object message,
                      int encodedBytes,
                      Object coalesceKey,
                      CompletableFuture<SendOutcome> result) {
            this.message = message;
            this.encodedBytes = encodedBytes;
            this.coalesceKey = coalesceKey;
            this.result = result;
        }
    }

    private static final class QueueLifecycleHandler extends ChannelDuplexHandler {
        private final OutboundPacketQueue owner;

        private QueueLifecycleHandler(OutboundPacketQueue owner) {
            this.owner = owner;
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            owner.onWritable(ctx.channel());
            super.channelWritabilityChanged(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            owner.onInactive(ctx.channel());
            super.channelInactive(ctx);
        }
    }
}

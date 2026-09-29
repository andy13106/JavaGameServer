package io.gameframe.runtime;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class RpcPendingCallsTest {
    @Test
    void completesOnceAndFiltersDuplicateAndUnknownReplies() {
        var calls = new RpcPendingCalls<String>(new RpcPendingCalls.Limits(2, 2));
        var future = calls.register("r1", 100, 0).toCompletableFuture();

        assertEquals(RpcPendingCalls.ReplyResult.COMPLETED, calls.complete("r1", "ok", 10));
        assertEquals("ok", future.join());
        assertEquals(RpcPendingCalls.ReplyResult.DUPLICATE, calls.complete("r1", "late", 11));
        assertEquals(RpcPendingCalls.ReplyResult.UNKNOWN, calls.complete("missing", "x", 11));
    }

    @Test
    void deadlineExpiresPendingCallAndLateReplyCannotWin() {
        var calls = new RpcPendingCalls<String>();
        var future = calls.register("r1", 50, 0).toCompletableFuture();

        assertEquals(1, calls.expire(50));
        var failure = assertThrows(CompletionException.class, future::join);
        assertInstanceOf(RpcPendingCalls.RpcDeadlineException.class, failure.getCause());
        assertEquals(RpcPendingCalls.ReplyResult.DUPLICATE, calls.complete("r1", "late", 51));
        assertEquals(0, calls.pendingCount());
    }

    @Test
    void pendingCapacityAndSettledWindowAreBounded() {
        var calls = new RpcPendingCalls<String>(new RpcPendingCalls.Limits(1, 1));
        calls.register("r1", 10, 0);
        assertThrows(IllegalStateException.class, () -> calls.register("r2", 10, 0));
        assertEquals(RpcPendingCalls.ReplyResult.COMPLETED, calls.complete("r1", "ok", 1));
        calls.register("r2", 20, 1);
        assertEquals(RpcPendingCalls.ReplyResult.COMPLETED, calls.complete("r2", "ok", 2));
        assertEquals(RpcPendingCalls.ReplyResult.UNKNOWN, calls.complete("r1", "old", 3));
    }

    @Test
    void closeFailsPendingCallsAndRejectsNewRegistration() {
        var calls = new RpcPendingCalls<String>();
        var future = calls.register("r1", 100, 0).toCompletableFuture();
        calls.close();
        assertThrows(CompletionException.class, future::join);
        assertThrows(IllegalStateException.class, () -> calls.register("r2", 100, 0));
    }
}
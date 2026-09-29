package io.gameframe.transport;

import com.zfoo.net.session.Session;
import com.zfoo.protocol.ProtocolManager;
import io.gameframe.runtime.RpcPendingCalls;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ZfooRpcProtocolTest {
    @BeforeAll
    static void initProtocol() {
        if (!ProtocolManager.isProtocolClass(RpcRequestEnvelope.class)) {
            ProtocolManager.initProtocol(Set.of(RpcRequestEnvelope.class, RpcResponseEnvelope.class, RpcPayload.class));
        }
    }

    @Test
    void requestEnvelopePreservesCorrelationAndPayload() {
        var request = new GameRpcClient.Request<RpcPayload>("battle-1", "command-9", new RpcPayload("hit"), 10_000);
        var envelope = ZfooRpcCodec.request(request);
        assertEquals("battle-1", envelope.getDestination());
        assertEquals("command-9", envelope.getCommandId());
        assertEquals(10_000, envelope.getDeadlineMillis());
        assertEquals("hit", ZfooRpcCodec.decode(envelope.getPayload(), RpcPayload.class).getValue());
    }

    @Test
    void responseRegistryCompletesAndFiltersDuplicate() throws Exception {
        try (var responses = new ZfooRpcPendingResponses(new RpcPendingCalls.Limits(8, 8))) {
            var future = responses.register("command-1", 5_000, 1_000).toCompletableFuture();
            assertEquals(RpcPendingCalls.ReplyResult.COMPLETED,
                    responses.accept(new RpcResponseEnvelope("command-1", true, null,
                            ZfooRpcCodec.encode(new RpcPayload("ok"))), 2_000));
            assertEquals("ok", ZfooRpcCodec.decode(future.get(1, TimeUnit.SECONDS), RpcPayload.class).getValue());
            assertEquals(RpcPendingCalls.ReplyResult.DUPLICATE,
                    responses.accept(new RpcResponseEnvelope("command-1", true, null, new byte[0]), 2_001));
        }
    }

    @Test
    void responseRegistryPropagatesRemoteFailure() {
        try (var responses = new ZfooRpcPendingResponses(new RpcPendingCalls.Limits(8, 8))) {
            var future = responses.register("command-2", 5_000, 1_000).toCompletableFuture();
            assertEquals(RpcPendingCalls.ReplyResult.COMPLETED,
                    responses.accept(new RpcResponseEnvelope("command-2", false, "rejected", null), 2_000));
            var error = assertThrows(Exception.class, future::join);
            assertTrue(error.getCause().getMessage().contains("rejected"));
        }
    }

    @Test
    void serverHandlerReturnsCorrelatedSuccessAndDeadlineFailure() {
        var channel = new EmbeddedChannel();
        var responses = new java.util.ArrayList<RpcResponseEnvelope>();
        var handler = new ZfooRpcServerHandler((session, response, attachment) -> {
            responses.add(response);
            return CompletableFuture.completedFuture(null);
        }, (session, request, attachment) ->
                CompletableFuture.completedFuture(ZfooRpcCodec.encode(new RpcPayload("reply"))));
        var session = new Session(channel);
        handler.dispatch(session, new RpcRequestEnvelope("battle", "cmd-ok",
                System.currentTimeMillis() + 5_000, new byte[0]), null);
        assertEquals("cmd-ok", responses.get(0).getCommandId());
        assertTrue(responses.get(0).isSuccess());
        assertEquals("reply", ZfooRpcCodec.decode(responses.get(0).getPayload(), RpcPayload.class).getValue());

        handler.dispatch(session, new RpcRequestEnvelope("battle", "cmd-late",
                System.currentTimeMillis() - 1, new byte[0]), null);
        assertEquals("cmd-late", responses.get(1).getCommandId());
        assertFalse(responses.get(1).isSuccess());
        assertTrue(responses.get(1).getError().contains("deadline"));
        channel.finishAndReleaseAll();
    }

    @Test
    void clientAdapterSendsEnvelopeAndDecodesCorrelatedResponse() throws Exception {
        var channel = new EmbeddedChannel();
        var sent = new java.util.ArrayList<RpcRequestEnvelope>();
        try (var adapter = new ZfooRpcClientAdapter(new Session(channel), null,
                (session, request, attachment) -> {
                    sent.add(request);
                    return CompletableFuture.completedFuture(null);
                })) {
            var future = adapter.call("battle", "cmd-client", new RpcPayload("hit"),
                    System.currentTimeMillis() + 5_000, RpcPayload.class,
                    io.gameframe.runtime.RpcCallExecutor.RetryMode.NEVER, error -> false).toCompletableFuture();
            assertEquals("cmd-client", sent.get(0).getCommandId());
            assertEquals("hit", ZfooRpcCodec.decode(sent.get(0).getPayload(), RpcPayload.class).getValue());
            assertEquals(io.gameframe.runtime.RpcPendingCalls.ReplyResult.COMPLETED,
                    adapter.accept(new RpcResponseEnvelope("cmd-client", true, null,
                            ZfooRpcCodec.encode(new RpcPayload("ok"))), System.currentTimeMillis()));
            assertEquals("ok", future.get(1, TimeUnit.SECONDS).getValue());
        } finally {
            channel.finishAndReleaseAll();
        }
    }}
package io.gameframe.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.util.concurrent.ScheduledFuture;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Adds bounded reliability, ordering and fragmentation around zfoo UDP datagrams.
 * One handler instance belongs to one UDP channel.
 */
public final class ReliableUdpChannelHandler extends MessageToMessageCodec<DatagramPacket, DatagramPacket> {
    public record Limits(int maxPeers, int maxFragmentPayload, Duration peerIdleTimeout,
                         Duration maintenanceInterval, ReliableUdpWindow.Limits window,
                         ReliableUdpFragments.Reassembler.Limits reassembly) {
        public Limits {
            Objects.requireNonNull(peerIdleTimeout, "peerIdleTimeout");
            Objects.requireNonNull(maintenanceInterval, "maintenanceInterval");
            Objects.requireNonNull(window, "window");
            Objects.requireNonNull(reassembly, "reassembly");
            if (maxPeers < 1 || maxFragmentPayload < 1
                    || maxFragmentPayload > ReliableUdpFrameCodec.MAX_FRAME_PAYLOAD
                    || peerIdleTimeout.isZero() || peerIdleTimeout.isNegative()
                    || maintenanceInterval.isZero() || maintenanceInterval.isNegative()) {
                throw new IllegalArgumentException("invalid reliable UDP channel limits");
            }
        }

        public static Limits defaults() {
            return new Limits(
                    16_384,
                    1_150,
                    Duration.ofMinutes(2),
                    Duration.ofMillis(25),
                    new ReliableUdpWindow.Limits(256, 512 * 1024L,
                            Duration.ofMillis(100), 5, 256),
                    new ReliableUdpFragments.Reassembler.Limits(
                            64, 2 * 1024 * 1024L, Duration.ofSeconds(5)));
        }
    }

    public record PeerReconnected(InetSocketAddress remote, long previousGeneration,
                                  long currentGeneration, int abandonedFragments) {}

    public record Snapshot(int peers, long malformedFrames, long rejectedFrames,
                           long deliveredDatagrams, long generationResets) {}

    private final Limits limits;
    private final long localGeneration;
    private final LongSupplier nanoTime;
    private final Map<InetSocketAddress, Peer> peers = new LinkedHashMap<>();
    private long malformedFrames;
    private long rejectedFrames;
    private long deliveredDatagrams;
    private long generationResets;
    private ScheduledFuture<?> maintenance;

    public ReliableUdpChannelHandler(Limits limits) {
        this(limits, ThreadLocalRandom.current().nextLong(), System::nanoTime);
    }

    ReliableUdpChannelHandler(Limits limits, long localGeneration, LongSupplier nanoTime) {
        this.limits = Objects.requireNonNull(limits, "limits");
        this.localGeneration = localGeneration;
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        long interval = limits.maintenanceInterval().toNanos();
        maintenance = ctx.executor().scheduleAtFixedRate(
                () -> maintain(ctx), interval, interval, TimeUnit.NANOSECONDS);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        cancelMaintenance();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelMaintenance();
        peers.clear();
        super.channelInactive(ctx);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> output) {
        InetSocketAddress recipient = packet.recipient();
        if (recipient == null) {
            rejectedFrames++;
            throw new ReliableUdpSendRejectedException("reliable UDP packet has no recipient");
        }
        long now = nanoTime.getAsLong();
        Peer peer = peer(recipient, now, true);
        if (peer == null) {
            rejectedFrames++;
            throw new ReliableUdpSendRejectedException("reliable UDP peer limit reached");
        }

        ByteBuf content = packet.content();
        byte[] payload = new byte[content.readableBytes()];
        content.getBytes(content.readerIndex(), payload);
        List<byte[]> fragments = ReliableUdpFragments.split(payload, limits.maxFragmentPayload());
        long wireBytes = payload.length + (long) fragments.size() * ReliableUdpFrameCodec.FRAME_OVERHEAD;
        var snapshot = peer.outbound.snapshot();
        if (fragments.size() > limits.window().maxPendingPackets() - snapshot.pendingPackets()
                || wireBytes > limits.window().maxPendingBytes() - snapshot.pendingBytes()) {
            rejectedFrames++;
            throw new ReliableUdpSendRejectedException("reliable UDP peer window is full");
        }

        if (!peer.helloSent) {
            output.add(encodeHello(ctx, peer.outboundGeneration, recipient));
            peer.helloSent = true;
        }

        long messageId = peer.nextMessageId;
        peer.nextMessageId = increment(peer.nextMessageId);
        for (int index = 0; index < fragments.size(); index++) {
            var pending = new PendingFragment(messageId, index, fragments.size(), fragments.get(index), recipient);
            var outbound = peer.outbound.offer(
                    pending, pending.payload.length + ReliableUdpFrameCodec.FRAME_OVERHEAD, now);
            if (outbound == null) {
                throw new IllegalStateException("reliable UDP preflight budget check became inconsistent");
            }
            output.add(encodeData(ctx, peer.outboundGeneration, outbound.sequence(), pending));
        }

    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket packet, List<Object> output) {
        ReliableUdpFrame frame;
        try {
            frame = ReliableUdpFrameCodec.decode(packet.content(), limits.maxFragmentPayload());
        } catch (IllegalArgumentException exception) {
            malformedFrames++;
            return;
        }

        InetSocketAddress sender = packet.sender();
        if (sender == null) {
            rejectedFrames++;
            return;
        }
        long now = nanoTime.getAsLong();
        Peer peer = peer(sender, now, frame.kind() == ReliableUdpFrame.Kind.DATA || frame.kind() == ReliableUdpFrame.Kind.HELLO);
        if (peer == null) {
            rejectedFrames++;
            return;
        }


        peer.lastTouchedNanos = now;

        if (frame.kind() == ReliableUdpFrame.Kind.ACK) {
            if (frame.sessionGeneration() != peer.outboundGeneration || !peer.outbound.acknowledge(frame.sequence())) {
                rejectedFrames++;
            }
            return;
        }

        if (frame.kind() == ReliableUdpFrame.Kind.HELLO) {
            handleHello(ctx, peer, sender, frame.sessionGeneration());
            return;
        }

        if (peer.remoteGeneration == null) {
            peer.resetInbound(frame.sessionGeneration(), limits);
        } else if (peer.remoteGeneration != frame.sessionGeneration()) {
            // A delayed packet from a previous incarnation must not roll the receive window back.
            // A fresh incarnation starts at sequence one.
            if (frame.sequence() != 1 || peer.retiredGenerations.contains(frame.sessionGeneration())) {
                rejectedFrames++;
                return;
            }
            long previous = peer.remoteGeneration;
            int abandoned = peer.resetForReconnect(frame.sessionGeneration(), limits);
            generationResets++;
            ctx.fireUserEventTriggered(new PeerReconnected(sender, previous,
                    frame.sessionGeneration(), abandoned));
        }
        var received = peer.inbound.receive(frame.sequence(), frame);
        if (received.kind() == ReliableUdpWindow.Kind.REJECTED) {
            rejectedFrames++;
            return;
        }
        writeAcknowledgement(ctx, sender, frame.sessionGeneration(), frame.sequence());
        if (!peer.helloSent) {
            writeHello(ctx, sender, peer.outboundGeneration);
            peer.helloSent = true;
        }
        for (var delivery : received.deliveries()) {
            var result = peer.reassembler.accept(delivery.payload(), now);
            if (result.status() == ReliableUdpFragments.Reassembler.Status.REJECTED) {
                rejectedFrames++;
                continue;
            }
            if (result.status() != ReliableUdpFragments.Reassembler.Status.COMPLETE) continue;
            byte[] complete = result.payload();
            ByteBuf body = ctx.alloc().buffer(complete.length).writeBytes(complete);
            output.add(new DatagramPacket(body, packet.recipient(), sender));
            deliveredDatagrams++;
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(peers.size(), malformedFrames, rejectedFrames,
                deliveredDatagrams, generationResets);
    }

    private Peer peer(InetSocketAddress address, long now, boolean create) {
        Peer peer = peers.get(address);
        if (peer != null || !create) return peer;
        evictIdle(now);
        if (peers.size() >= limits.maxPeers()) return null;
        peer = new Peer(limits, now, localGeneration);
        peers.put(address, peer);
        return peer;
    }

    private void maintain(ChannelHandlerContext ctx) {
        long now = nanoTime.getAsLong();
        boolean wrote = false;
        for (Peer peer : peers.values()) {
            peer.reassembler.expire(now);
            for (var outbound : peer.outbound.due(now)) {
                ctx.write(encodeData(ctx, peer.outboundGeneration, outbound.sequence(), outbound.payload()));
                wrote = true;
            }
        }
        evictIdle(now);
        if (wrote) ctx.flush();
    }

    private DatagramPacket encodeData(ChannelHandlerContext ctx, long generation, long sequence, PendingFragment pending) {
        var frame = ReliableUdpFrame.data(generation, sequence, pending.messageId,
                pending.fragmentIndex, pending.fragmentCount, pending.payload);
        return new DatagramPacket(ReliableUdpFrameCodec.encode(ctx.alloc(), frame), pending.recipient);
    }

    private DatagramPacket encodeHello(ChannelHandlerContext ctx, long generation, InetSocketAddress recipient) {
        var frame = ReliableUdpFrame.hello(generation);
        return new DatagramPacket(ReliableUdpFrameCodec.encode(ctx.alloc(), frame), recipient);
    }

    private void writeAcknowledgement(ChannelHandlerContext ctx, InetSocketAddress recipient,
                                      long generation, long sequence) {
        var frame = ReliableUdpFrame.acknowledgement(generation, sequence);
        ctx.writeAndFlush(new DatagramPacket(ReliableUdpFrameCodec.encode(ctx.alloc(), frame), recipient));
    }

    private void writeHello(ChannelHandlerContext ctx, InetSocketAddress recipient, long generation) {
        var frame = ReliableUdpFrame.hello(generation);
        ctx.writeAndFlush(new DatagramPacket(ReliableUdpFrameCodec.encode(ctx.alloc(), frame), recipient));
    }

    private void handleHello(ChannelHandlerContext ctx, Peer peer, InetSocketAddress sender, long generation) {
        if (peer.remoteGeneration == null) {
            peer.resetInbound(generation, limits);
            return;
        }
        if (peer.remoteGeneration == generation || peer.retiredGenerations.contains(generation)) {
            if (peer.retiredGenerations.contains(generation)) rejectedFrames++;
            return;
        }
        long previous = peer.remoteGeneration;
        int abandoned = peer.resetForReconnect(generation, limits);
        generationResets++;
        ctx.fireUserEventTriggered(new PeerReconnected(sender, previous, generation, abandoned));
    }

    private void evictIdle(long now) {
        var iterator = peers.entrySet().iterator();
        while (iterator.hasNext()) {
            Peer peer = iterator.next().getValue();
            boolean idle = now - peer.lastTouchedNanos >= limits.peerIdleTimeout().toNanos();
            if (idle && peer.outbound.snapshot().pendingPackets() == 0) iterator.remove();
        }
    }

    private void cancelMaintenance() {
        if (maintenance != null) {
            maintenance.cancel(false);
            maintenance = null;
        }
    }

    private static long increment(long value) {
        return (value + 1) & ReliableUdpWindow.MAX_SEQUENCE;
    }

    private record PendingFragment(long messageId, int fragmentIndex, int fragmentCount,
                                   byte[] payload, InetSocketAddress recipient) {
        private PendingFragment {
            payload = payload.clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }

    private static final class Peer {
        private ReliableUdpWindow<PendingFragment> outbound;
        private final ArrayDeque<Long> retiredGenerations = new ArrayDeque<>();
        private long outboundGeneration;
        private boolean helloSent;
        private ReliableUdpWindow<ReliableUdpFrame> inbound;
        private ReliableUdpFragments.Reassembler reassembler;
        private Long remoteGeneration;
        private long nextMessageId = 1;
        private long lastTouchedNanos;

        private Peer(Limits limits, long now, long generation) {
            outboundGeneration = generation;
            outbound = new ReliableUdpWindow<>(limits.window());
            inbound = new ReliableUdpWindow<>(limits.window());
            reassembler = new ReliableUdpFragments.Reassembler(limits.reassembly());
            lastTouchedNanos = now;
        }

        private void resetInbound(long generation, Limits limits) {
            remoteGeneration = generation;
            inbound = new ReliableUdpWindow<>(limits.window());
            reassembler = new ReliableUdpFragments.Reassembler(limits.reassembly());
        }

        private int resetForReconnect(long generation, Limits limits) {
            int abandoned = (int) outbound.snapshot().pendingPackets();
            retiredGenerations.addLast(remoteGeneration);
            if (retiredGenerations.size() > 8) retiredGenerations.removeFirst();
            outbound = new ReliableUdpWindow<>(limits.window());
            long nextGeneration;
            do { nextGeneration = ThreadLocalRandom.current().nextLong(); }
            while (nextGeneration == outboundGeneration);
            outboundGeneration = nextGeneration;
            helloSent = false;
            nextMessageId = 1;
            resetInbound(generation, limits);
            return abandoned;
        }
    }
}

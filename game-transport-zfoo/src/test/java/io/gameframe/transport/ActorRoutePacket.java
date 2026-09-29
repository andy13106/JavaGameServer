package io.gameframe.transport;

import com.zfoo.protocol.anno.Protocol;

@Protocol(id = 32001)
public class ActorRoutePacket {
    private int sequence;

    public ActorRoutePacket() {
    }

    public ActorRoutePacket(int sequence) {
        this.sequence = sequence;
    }

    public int getSequence() {
        return sequence;
    }

    public void setSequence(int sequence) {
        this.sequence = sequence;
    }
}


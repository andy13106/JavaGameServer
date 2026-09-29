package io.gameframe.transport;

import com.zfoo.protocol.anno.Protocol;

/** Wire envelope carrying a stable command id and an already encoded zfoo payload. */
@Protocol(id = 31001)
public class RpcRequestEnvelope {
    private String destination;
    private String commandId;
    private long deadlineMillis;
    private byte[] payload;

    public RpcRequestEnvelope() {
    }

    public RpcRequestEnvelope(String destination, String commandId, long deadlineMillis, byte[] payload) {
        this.destination = destination;
        this.commandId = commandId;
        this.deadlineMillis = deadlineMillis;
        this.payload = payload;
    }

    public String getDestination() { return destination; }
    public void setDestination(String destination) { this.destination = destination; }
    public String getCommandId() { return commandId; }
    public void setCommandId(String commandId) { this.commandId = commandId; }
    public long getDeadlineMillis() { return deadlineMillis; }
    public void setDeadlineMillis(long deadlineMillis) { this.deadlineMillis = deadlineMillis; }
    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
}
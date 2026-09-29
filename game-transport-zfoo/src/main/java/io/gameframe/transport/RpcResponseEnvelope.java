package io.gameframe.transport;

import com.zfoo.protocol.anno.Protocol;

/** Wire envelope carrying a response correlated by command id. */
@Protocol(id = 31002)
public class RpcResponseEnvelope {
    private String commandId;
    private boolean success;
    private String error;
    private byte[] payload;

    public RpcResponseEnvelope() {
    }

    public RpcResponseEnvelope(String commandId, boolean success, String error, byte[] payload) {
        this.commandId = commandId;
        this.success = success;
        this.error = error;
        this.payload = payload;
    }

    public String getCommandId() { return commandId; }
    public void setCommandId(String commandId) { this.commandId = commandId; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getError() { return error; }
    public void setError(String error) { this.error = error; }
    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
}
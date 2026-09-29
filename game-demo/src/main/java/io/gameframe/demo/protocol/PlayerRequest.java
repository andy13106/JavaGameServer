package io.gameframe.demo.protocol;
import com.zfoo.protocol.anno.Protocol;
@Protocol(id = 20001)
public class PlayerRequest {
    private String requestId;
    private String action;
    private long amount;
    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public long getAmount() { return amount; }
    public void setAmount(long amount) { this.amount = amount; }
}

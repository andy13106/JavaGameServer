package io.gameframe.demo.protocol;
import com.zfoo.protocol.anno.Protocol;
@Protocol(id = 20002)
public class PlayerResponse {
    private String requestId;
    private boolean success;
    private long version;
    private long gold;
    private String message;
    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public long getGold() { return gold; }
    public void setGold(long gold) { this.gold = gold; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
}

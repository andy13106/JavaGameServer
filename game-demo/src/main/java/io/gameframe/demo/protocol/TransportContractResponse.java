package io.gameframe.demo.protocol;

import com.zfoo.protocol.anno.Protocol;

@Protocol(id = 20004)
public class TransportContractResponse {
    private String marker;
    private boolean success;

    public String getMarker() { return marker; }
    public void setMarker(String marker) { this.marker = marker; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
}
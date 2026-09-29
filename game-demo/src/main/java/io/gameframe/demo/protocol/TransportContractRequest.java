package io.gameframe.demo.protocol;

import com.zfoo.protocol.anno.Protocol;

@Protocol(id = 20003)
public class TransportContractRequest {
    private String marker;

    public String getMarker() { return marker; }
    public void setMarker(String marker) { this.marker = marker; }
}
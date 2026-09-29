package io.gameframe.transport;

import com.zfoo.protocol.anno.Protocol;

@Protocol(id = 31003)
public class RpcPayload {
    private String value;

    public RpcPayload() {
    }

    public RpcPayload(String value) {
        this.value = value;
    }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }
}
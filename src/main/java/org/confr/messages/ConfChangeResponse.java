package org.confr.messages;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class ConfChangeResponse extends ConfrMessage {
    public static final short VERSION = 1;
    public static Builder builder() {
        return new Builder();
    }

    public static ConfChangeResponse readFrom(ByteBuf buf) {
        return new ConfChangeResponse(buf);
    }

    private Map<String, String> data;
    ConfChangeResponse(String clientId, Map<String, String> data) {
        super(MessageType.ConfChangeResponse, VERSION, -1, clientId);
        this.data = data;
    }

    private ConfChangeResponse(ByteBuf buf) {
        super(buf, MessageType.ConfChangeResponse);
        int mapsize = buf.readInt();

        // NOTICE: do not init capicity with mapsize.
        this.data = new HashMap<>();
        for (int i = 0; i < mapsize; i++) {
            String k = buf.readBytes(buf.readInt()).toString(StandardCharsets.UTF_8);
            String v = buf.readBytes(buf.readInt()).toString(StandardCharsets.UTF_8);
            data.put(k, v);
        }
    }

    public Map<String, String> getData() {
        return data;
    }

    @Override public long writeTo(ByteBuf buf) throws IOException {
        int writerIndex = buf.writerIndex();
        writeHeader(buf);
        buf.writeInt(data.size());
        for (Map.Entry<String, String> entry : data.entrySet()) {
            buf.writeInt(entry.getKey().getBytes().length).writeBytes(entry.getKey().getBytes());
            buf.writeInt(entry.getValue().getBytes().length).writeBytes(entry.getValue().getBytes());
        }
        return buf.writerIndex() - writerIndex;
    }

    public static class Builder {
        private String clientId;
        private Map<String, String> configData = new HashMap<>();


        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder addConfigData(String key, String value) {
            configData.put(key, value);
            return this;
        }

        public Builder addConfigData(Map<String, String> config) {
            configData.putAll(config);
            return this;
        }

        public ConfChangeResponse build() {
            return new ConfChangeResponse(clientId, configData);
        }
    }
}

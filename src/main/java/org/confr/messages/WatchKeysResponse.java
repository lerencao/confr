package org.confr.messages;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class WatchKeysResponse extends ConfrMessage {
    public static Builder builder() {
        return new Builder();
    }
    public static WatchKeysResponse readFrom(ByteBuf buf) {
        return new WatchKeysResponse(buf);
    }


    private static final short VERSION = 1;
    private Map<String, String> configData = new HashMap<>();


    private WatchKeysResponse(long correlationId, String clientId, Map<String, String> configData) {
        super(MessageType.WatchKeysResponse, VERSION, correlationId, clientId);
        this.configData = configData;
    }

    private WatchKeysResponse(ByteBuf buf) {
        super(buf, MessageType.WatchKeysResponse);
        int mapsize = buf.readInt();

        // NOTICE: do not init capicity with mapsize.
        this.configData = new HashMap<>();
        for (int i = 0; i < mapsize; i++) {
            String k = buf.readBytes(buf.readInt()).toString(StandardCharsets.UTF_8);
            String v = buf.readBytes(buf.readInt()).toString(StandardCharsets.UTF_8);
            configData.put(k, v);
        }
    }

    public Map<String, String> getConfigData() {
        return configData;
    }

    @Override public long writeTo(ByteBuf buf) throws IOException {
        int writerIndex = buf.writerIndex();
        writeHeader(buf);
        buf.writeInt(configData.size());
        for (Map.Entry<String, String> entry : configData.entrySet()) {
            buf.writeInt(entry.getKey().getBytes().length).writeBytes(entry.getKey().getBytes());
            buf.writeInt(entry.getValue().getBytes().length).writeBytes(entry.getValue().getBytes());
        }
        return buf.writerIndex() - writerIndex;
    }


    public static class Builder {
        private String clientId;
        private long correlationId;

        private Map<String, String> configData = new HashMap<>();


        public Builder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder setCorrelationId(long correlationId) {
            this.correlationId = correlationId;
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

        public WatchKeysResponse build() {
            return new WatchKeysResponse(correlationId, clientId, configData);
        }
    }
}

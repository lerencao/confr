package org.confr.messages;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class WatchKeysRequest extends ConfrMessage {

    public static WatchKeysRequestBuilder builder() {
        return new WatchKeysRequestBuilder();
    }

    public static WatchKeysRequest readFrom(ByteBuf buf) {
        return new WatchKeysRequest(buf);
    }

    private static final short VERSION = 1;

    private final Set<String> keys;

    WatchKeysRequest(long correlationId, String clientId, Set<String> keys) {
        super(MessageType.WatchKeysRequest, VERSION, correlationId, clientId);
        this.keys = keys;
    }

    // init request from buf
    private WatchKeysRequest(ByteBuf buf) {
        super(buf, MessageType.WatchKeysRequest);
        int size = buf.readInt();
        this.keys = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            keys.add(buf.readBytes(buf.readInt()).toString(StandardCharsets.UTF_8));
        }
    }

    public Set<String> getKeys() {
        return keys;
    }

    @Override public long writeTo(ByteBuf buf) throws IOException {
        int writerStart = buf.writerIndex();
        writeHeader(buf);

        // write keys
        buf.writeInt(keys.size());
        for (String key : keys) {
            buf.writeInt(key.getBytes().length).writeBytes(key.getBytes());
        }

        return buf.writerIndex() - writerStart;
    }


    public static class WatchKeysRequestBuilder {
        private long correlationId;
        private String clientId;
        private Set<String> keys = new HashSet<>();
        public WatchKeysRequestBuilder setCorrelationId(long correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public WatchKeysRequestBuilder setClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public WatchKeysRequestBuilder addWatchKey(String key) {
            keys.add(key);
            return this;
        }

        public WatchKeysRequestBuilder addWatchKeys(Collection<String> keys) {
            this.keys.addAll(keys);
            return this;
        }

        public WatchKeysRequest build() {
            return new WatchKeysRequest(correlationId, clientId, keys);
        }

    }

}

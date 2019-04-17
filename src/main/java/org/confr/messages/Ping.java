package org.confr.messages;

import io.netty.buffer.ByteBuf;

public class Ping extends ConfrMessage {
    private static final String PING = "PING";
    public static Ping readFrom(ByteBuf buf) {
        return new Ping(buf);
    }

    public Ping(short versionId,long correlationId, String clientId) {
        super(MessageType.PING, versionId, correlationId, clientId);
    }

    private Ping(ByteBuf buf) {
        super(buf, MessageType.PING);
        buf.skipBytes(buf.readInt());
    }

    @Override public long writeTo(ByteBuf buf) {
        int writeStartIdx = buf.writerIndex();

        writeHeader(buf);

        byte[] bytes = PING.getBytes();
        buf.writeInt(bytes.length).writeBytes(bytes);

        return buf.writerIndex() - writeStartIdx;
    }
}

package org.confr.messages;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

public class Pong extends ConfrMessage {
    private static final String PONG = "PONG";

    public static Pong readFrom(ByteBuf buf) {
        return new Pong(buf);
    }


    public Pong(short versionId, long correlationId, String clientId) {
        super(MessageType.PONG, versionId, correlationId, clientId);
    }

    private Pong(ByteBuf buf) {
        super(buf, MessageType.PONG);
        buf.skipBytes(buf.readInt());
    }

    @Override public long writeTo(ByteBuf buf) throws IOException {
        int writeStartIdx = buf.writerIndex();
        writeHeader(buf);

        byte[] bytes = PONG.getBytes();
        buf.writeInt(bytes.length).writeBytes(bytes);

        return buf.writerIndex() - writeStartIdx;
    }
}

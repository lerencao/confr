package org.confr.messages;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public abstract class ConfrMessage implements Send {
    private final MessageType type;
    private final short versionId;
    private final String clientId;
    private final long correlationId;

    private static final int Message_Type_Size_In_Bytes = Integer.BYTES;
    private static final int Message_Version_Size_In_Bytes = Short.BYTES;
    private static final int CorrelationId_Size_In_Bytes = Long.BYTES;
    private static final int ClientId_Field_Size_In_Bytes = Integer.BYTES;

    public ConfrMessage(MessageType type, short versionId, long correlationId, String clientId) {
        this.type = type;
        this.versionId = versionId;
        this.clientId = clientId;
        this.correlationId = correlationId;
    }

    protected ConfrMessage(ByteBuf buf, MessageType type) {
        this.type = type;
        this.versionId = buf.readShort();
        this.correlationId = buf.readLong();
        this.clientId = buf.readBytes(buf.readInt()).toString(StandardCharsets.UTF_8);
    }

    public MessageType getType() {
        return type;
    }

    public short getVersionId() {
        return versionId;
    }

    public long getCorrelationId() {
        return correlationId;
    }

    public String getClientId() {
        return clientId;
    }

    void writeHeader(ByteBuf buffer) {
        buffer.writeInt(type.ordinal())
        .writeShort(versionId)
        .writeLong(correlationId)
        .writeInt(clientId.getBytes().length).writeBytes(clientId.getBytes());
    }

    private int headerSize() {
        return Message_Type_Size_In_Bytes +
                Message_Version_Size_In_Bytes +
                CorrelationId_Size_In_Bytes +
                ClientId_Field_Size_In_Bytes + clientId.getBytes().length;
    }
}

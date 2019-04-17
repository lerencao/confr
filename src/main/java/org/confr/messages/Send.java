package org.confr.messages;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.WritableByteChannel;

public interface Send {
//    long writeTo(WritableByteChannel channel) throws IOException;
    long writeTo(ByteBuf buf) throws IOException;
//    long sizeInBytes();
}

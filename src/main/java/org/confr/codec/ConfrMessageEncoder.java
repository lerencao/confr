package org.confr.codec;

import org.confr.messages.ConfrMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConfrMessageEncoder extends MessageToByteEncoder<ConfrMessage> {
    private static final Logger logger = LoggerFactory.getLogger(ConfrMessageEncoder.class);
    @Override protected void encode(ChannelHandlerContext ctx, ConfrMessage msg, ByteBuf out) throws Exception {
        logger.trace("Encode conf message {}", msg.getType());
        msg.writeTo(out);
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("Exception caught", cause);
        super.exceptionCaught(ctx, cause);
    }
}

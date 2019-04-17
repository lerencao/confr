package org.confr.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import org.confr.messages.ConfChangeResponse;
import org.confr.messages.MessageType;
import org.confr.messages.Ping;
import org.confr.messages.Pong;
import org.confr.messages.WatchKeysRequest;
import org.confr.messages.WatchKeysResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *  * Decode ConfrMessage from bytebuf.
 */
public class ConfrMessageDecoder extends ByteToMessageDecoder {
    private static final Logger logger = LoggerFactory.getLogger(ConfrMessageDecoder.class);
    @Override protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // read all
        ByteBuf byteBuf = in.readSlice(in.readableBytes());

        int ordinal = byteBuf.readInt();
        if (ordinal < 0 || ordinal > MessageType.values().length - 1) {
            logger.error("Receive invalid message of type {}, close channel now", ordinal);
            throw new DecoderException("Invalid message type id " + ordinal);
        }

        MessageType messageType = MessageType.values()[ordinal];

        switch (messageType) {
        case PING:
            out.add(Ping.readFrom(byteBuf));
            break;
        case PONG:
            out.add(Pong.readFrom(byteBuf));
            break;
        case WatchKeysRequest:
            out.add(WatchKeysRequest.readFrom(byteBuf));
            break;
        case WatchKeysResponse:
            out.add(WatchKeysResponse.readFrom(byteBuf));
            break;
        case ConfChangeResponse:
            out.add(ConfChangeResponse.readFrom(byteBuf));
            break;
        default:
            throw new UnsupportedOperationException("Message type not supported " + messageType);
        }
    }
}

package org.confr.server;

import org.confr.codec.ConfrMessageDecoder;
import org.confr.codec.ConfrMessageEncoder;
import org.confr.messages.ConfrMessage;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyServerChannelInitializer extends ChannelInitializer<SocketChannel> {
    private NettyRequestResponseChannel requestResponseChannel;
    NettyServerChannelInitializer(NettyRequestResponseChannel requestResponseChannel) {
        this.requestResponseChannel = requestResponseChannel;
    }

    @Override protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        // outbound handlers
//        pipeline.addLast("debugger", new ChannelOutboundDebuger());
        pipeline.addLast("lengthFieldPrepender", new LengthFieldPrepender(4, true));
        pipeline.addLast("confrMessageEncoder", new ConfrMessageEncoder());

        // inbound handler
        pipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, 30));
//        pipeline.addLast("debugChannelInboundHandler", new ChannelInboundDebuger());
        pipeline.addLast("lengthFieldBasedFrameHandler", new LengthFieldBasedFrameDecoder(4 * 1024 * 1024, 0, 4, -4, 4))
                .addLast("confrMessageDecoder", new ConfrMessageDecoder());
        pipeline.addLast("processor", new ConfrMessageServerProcessor(requestResponseChannel));
    }
}

@ChannelHandler.Sharable
class ConfrMessageServerProcessor extends SimpleChannelInboundHandler<ConfrMessage> {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private NettyRequestResponseChannel requestResponseChannel;
    ConfrMessageServerProcessor(NettyRequestResponseChannel requestResponseChannel) {
        this.requestResponseChannel = requestResponseChannel;
    }

    @Override protected void channelRead0(ChannelHandlerContext ctx, ConfrMessage msg) throws Exception {
        requestResponseChannel.sendCommand(new RequestInfo<ConfrMessage>(ctx.channel(), msg));
    }

    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            logger.warn("{}, {}: Close channel due to idle too long", this, ctx.channel());
            // send a null message reqeust to indicate that the channel close
            requestResponseChannel.sendCommand(new ClearSessionCmd(ctx.channel()));
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.warn("{}: Channel closed, singal cleanup", ctx.channel().id());
        requestResponseChannel.sendCommand(new ClearSessionCmd(ctx.channel()));
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof DecoderException) {
            // Close channel and clear session
            ctx.channel().close();
            requestResponseChannel.sendCommand(new ClearSessionCmd(ctx.channel()));
        } else {
            super.exceptionCaught(ctx, cause);
        }

    }
}


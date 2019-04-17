package org.confr.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelInboundDebuger extends ChannelInboundHandlerAdapter {
    public static final Logger logger = LoggerFactory.getLogger(ChannelInboundDebuger.class);

    @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: channel registered", ctx.channel());
        super.channelRegistered(ctx);
    }

    @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: channel unregistered", ctx.channel());
        super.channelUnregistered(ctx);
    }

    @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: channel active", ctx.channel());
        super.channelActive(ctx);
    }

    @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: channel inactive", ctx.channel());
        super.channelInactive(ctx);
    }

    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        logger.trace("{}: channel read: {}", ctx.channel(), msg.getClass());
        super.channelRead(ctx, msg);
    }

    @Override public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: channel read complete: {}", ctx.channel());
        super.channelReadComplete(ctx);
    }

    @Override public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: channel writability changed", ctx.channel());
        super.channelWritabilityChanged(ctx);
    }

    @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        logger.info("{}: user event {}: {}", ctx.channel(), evt.getClass(), evt);
        super.userEventTriggered(ctx, evt);
    }

    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("{}: exception caught {}", ctx.channel(), cause);
        super.exceptionCaught(ctx, cause);
    }
}

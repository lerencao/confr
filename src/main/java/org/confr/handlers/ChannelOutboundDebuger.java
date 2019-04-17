package org.confr.handlers;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

public class ChannelOutboundDebuger extends ChannelOutboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ChannelOutboundDebuger.class);
    @Override public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise)
            throws Exception {
        logger.trace("{}: bind on {}", ctx.channel(), localAddress);
        super.bind(ctx, localAddress, promise);
    }

    @Override public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
            ChannelPromise promise) throws Exception {
        logger.trace("{}: connect to {} on {}", ctx.channel(), remoteAddress, localAddress);
        super.connect(ctx, remoteAddress, localAddress, promise);
    }

    @Override public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        logger.trace("{}: disconnect", ctx.channel());
        super.disconnect(ctx, promise);
    }

    @Override public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        logger.trace("{}: close", ctx.channel());
        super.close(ctx, promise);
    }

    @Override public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        logger.trace("{}: deregister", ctx.channel());
        super.deregister(ctx, promise);
    }
    @Override public void read(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: read event", ctx.channel());
        super.read(ctx);
    }

    @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        logger.trace("{}: write {}", ctx.channel(), msg);
        super.write(ctx, msg, promise);
    }

    @Override public void flush(ChannelHandlerContext ctx) throws Exception {
        logger.trace("{}: flush", ctx.channel());
        super.flush(ctx);
    }
}

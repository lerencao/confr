package org.confr.client;

import org.confr.codec.ConfrMessageDecoder;
import org.confr.codec.ConfrMessageEncoder;
import org.confr.messages.ConfrMessage;
import org.confr.messages.Ping;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NettyClientChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final Logger logger = LoggerFactory.getLogger(NettyClientChannelInitializer.class);

    private NettyClient nettyClient;
    public NettyClientChannelInitializer(NettyClient client) {
        this.nettyClient = client;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        // outbound handlers
//        pipeline.addLast(new ChannelOutboundDebuger());
        pipeline.addLast(new LengthFieldPrepender(4, true));
        pipeline.addLast(new ConfrMessageEncoder());

        // inbound handlers
        pipeline.addLast("idleStateHandler", new IdleStateHandler(0, 0, 5));
//        pipeline.addLast(new ChannelInboundDebuger());
        pipeline.addLast("idleEventHandler", new IdleEventClientHandler());
        pipeline.addLast("reconnectHandler", new ReconnectRemoteHandler());
        pipeline.addLast("lengthFieldBasedFrameDecoder", new LengthFieldBasedFrameDecoder(4 * 1024 * 1024, 0, 4, -4, 4))
                .addLast("confrMessageDecoder", new ConfrMessageDecoder());
        pipeline.addLast("confrMessageClientHandler", new ConfrMessageClientHandler(nettyClient.getResponseHandler()));
    }

    /**
     * Created by changyang at 25/07/2017
     * All men must die, all men must serve.
     *
     * Reconnect remote server every 1s when channel is inactive, see client.reconnect
     */
    @Sharable
    class ReconnectRemoteHandler extends ChannelInboundHandlerAdapter {
        @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            logger.info("{}: Disconnect from server", ctx.channel());
            if (!ctx.channel().eventLoop().isShuttingDown()) {
                logger.warn("{}: Reconnect to server", ctx.channel());
                nettyClient.reconnect();
            }
            super.channelInactive(ctx);
        }

    }


    @Sharable
    class IdleEventClientHandler extends ChannelInboundHandlerAdapter {
        @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                // write and flush immediately
                ctx.writeAndFlush(new Ping((short)1, 1, "client1"));
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    @Sharable
    static class ConfrMessageClientHandler extends SimpleChannelInboundHandler<ConfrMessage> {
        private static final Logger logger = LoggerFactory.getLogger(ConfrMessageClientHandler.class);
        private ResponseHandler responseHandler;

        ConfrMessageClientHandler(ResponseHandler responseHandler) {
            this.responseHandler = responseHandler;
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, ConfrMessage msg) throws Exception {
            logger.trace("{}: read msg {}", ctx.channel(), msg.getType());
            responseHandler.handleResponse(new ConfrResponseInfo(msg, ctx.channel()));
        }
    }
}

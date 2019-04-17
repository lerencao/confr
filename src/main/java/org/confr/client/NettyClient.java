package org.confr.client;

import org.confr.messages.ConfrMessage;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class NettyClient {
    private static final Logger logger = LoggerFactory.getLogger(NettyClient.class);

    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private NioEventLoopGroup workerGroup;
    private Bootstrap bootstrap;
    private SocketAddress address;

    private Channel channel;
    private final ResponseHandler responseHandler;

    // called when server connected or reconnected
    private final List<ServerReconnectedListener> serverReconnectedListeners = new LinkedList<>();
    private final ReconnectChannelFutureListener reconnectChannelFutureListener = new ReconnectChannelFutureListener();


    NettyClient(SocketAddress address, ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        this.address = address;
    }

    void start() throws InstantiationException {
        long startupBeginTime = System.currentTimeMillis();
        try {
            logger.trace("Starting NettyClient");
            workerGroup = new NioEventLoopGroup(1);
            bootstrap = new Bootstrap()
                    .group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new NettyClientChannelInitializer(this));

            // connect to remote server
            // TODO: reconnect if failed? and connect to other server?
            reconnect().sync();

            long startupTime = System.currentTimeMillis() - startupBeginTime;
            logger.info("NettyClient start took {} ms", startupTime);

            // TODO(changyang): batch watch?
//            logger.debug("Begin to watch configs");
//            for (String config : session.getWatchedConfig()) {
//                watch(config).join();
//            }
//            logger.debug("Finish watch configs");

        }  catch (InterruptedException e) {
            logger.error("NettyClient start await was interrupted", e);
            throw new InstantiationException(
                    "Netty client connect to port [" + 8844 + "] was interrupted");
        }
    }

    void shutdown() {
        logger.info("Shutting down NettyClient");
        if (workerGroup != null && !workerGroup.isTerminated()) {
            long shutdownBeginTime = System.currentTimeMillis();
            try {
                if (!workerGroup.shutdownGracefully().await(30, TimeUnit.SECONDS)) {
                    logger.error("NettyClient shutdown failed after waiting for 30 seconds");
                }
            } catch (InterruptedException e) {
                logger.error("NettyClient termination await was interrupted. Shutdown may have been unsuccessful", e);
            } finally {
                long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
                logger.info("NettyServer shutdown took {} ms", shutdownTime);

            }
        }

        shutdownLatch.countDown();
    }

    void awaitShutdown() throws InterruptedException {
        this.shutdownLatch.await();
    }

    public void addReconnectListener(ServerReconnectedListener listener) {
        this.serverReconnectedListeners.add(listener);
    }

    public void removeReconnectListener(ServerReconnectedListener listener) {
        this.serverReconnectedListeners.remove(listener);
    }

    ChannelFuture reconnect() {
        if (channel != null && channel.isActive()) {
            return channel.newSucceededFuture();
        }
        return bootstrap.connect(address).addListener(reconnectChannelFutureListener);
    }

    public Channel getChannel() {
        return channel;
    }


    ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    ChannelFuture send(ConfrMessage message) {
        if (channel == null || !channel.isActive()) {
            throw new IllegalStateException("Channel is not opened");
        } else if (!channel.isWritable()) {
            throw new IllegalStateException("Channel is not writable");
        } else {
            return channel.writeAndFlush(message);
        }
    }

    class ReconnectChannelFutureListener implements ChannelFutureListener {
        @Override public void operationComplete(ChannelFuture future) throws Exception {
            if (!future.isSuccess()) {
                logger.warn("{}: Failed to Reconnect", future.channel());
                future.channel().eventLoop().schedule(
                        NettyClient.this::reconnect,
                        1000, TimeUnit.MILLISECONDS
                );
            } else {
                channel = future.channel();
                for (ServerReconnectedListener listener : serverReconnectedListeners) {
                    try {
                        listener.onConnected();
                    } catch (Exception e) {
                        logger.error("Failed to call listener", e);
                    }
                }
                logger.info("{}: Succuess to reconnect", future.channel());
            }
        }
    }

}

interface ServerReconnectedListener {
    void onConnected();
}


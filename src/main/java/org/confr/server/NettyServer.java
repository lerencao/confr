package org.confr.server;
import org.confr.utils.ThreadUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class NettyServer {
    //    private final NettyConfig nettyConfig;
    //    private final NettyMetrics nettyMetrics;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ChannelInitializer<SocketChannel> channelInitializer;
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private SocketAddress address;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    private NettyRequestResponseChannel requestResponseChannel;
    private ResponseHandler responseHandler;
    /**
     * Creates a new instance of NettyServer.
     */
    public NettyServer(SocketAddress address) throws IOException {
        this.address = address;

        this.requestResponseChannel = new NettyRequestResponseChannel(500); // TODO: make it configurable
        this.channelInitializer = new NettyServerChannelInitializer(requestResponseChannel);

        this.responseHandler = new ResponseHandler(requestResponseChannel);
        logger.trace("Instantiated NettyServer");
    }

    public NettyRequestResponseChannel getRequestResponseChannel() {
        return requestResponseChannel;
    }

    public void start() throws InstantiationException {
        long startupBeginTime = System.currentTimeMillis();
        try {
            logger.trace("Starting NettyServer deployment");
            bossGroup = new NioEventLoopGroup(1);
            workerGroup = new NioEventLoopGroup(1);
            bindServer(address, channelInitializer, bossGroup, workerGroup);
            ThreadUtils.newThread("response-handler", responseHandler, false).start();
        } catch (InterruptedException e) {
            logger.error("NettyServer start await was interrupted", e);
            throw new InstantiationException(
                    "Netty server bind to address [" + address + "] was interrupted");
        } finally {
            long startupTime = System.currentTimeMillis() - startupBeginTime;
            logger.info("NettyServer start took {} ms", startupTime);
        }
    }

    public void shutdown() {
        logger.info("Shutting down NettyServer");
        long shutdownBeginTime = System.currentTimeMillis();
        try {
            if (bossGroup != null && workerGroup != null && (!bossGroup.isTerminated() || !workerGroup.isTerminated())) {
                workerGroup.shutdownGracefully();
                bossGroup.shutdownGracefully();

                if (!(workerGroup.awaitTermination(30, TimeUnit.SECONDS) && bossGroup.awaitTermination(30, TimeUnit.SECONDS))) {
                    logger.error("NettyServer shutdown failed after waiting for 30 seconds");
                }
            }

            if (responseHandler != null) {
                responseHandler.shutdown();
            }
        } catch (InterruptedException e) {
            logger.error("NettyServer termination await was interrupted. Shutdown may have been unsuccessful", e);
        } finally {
            long shutdownTime = System.currentTimeMillis() - shutdownBeginTime;
            logger.info("NettyServer shutdown took {} ms", shutdownTime);
        }

        shutdownLatch.countDown();
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

    /**
     * Bootstrap a new server with a {@link ChannelInitializer} and bind it to a port.
     * @param address the address to bind this server to.
     * @param channelInitializer the {@link ChannelInitializer} for request handling on this server.
     * @param bossGroup the pool of boss threads that this server uses.
     * @param workerGroup the pool of worker threads that this server uses.
     * @throws InterruptedException if binding to the port failed.
     */
    private void bindServer(SocketAddress address, ChannelInitializer<SocketChannel> channelInitializer, EventLoopGroup bossGroup,
            EventLoopGroup workerGroup) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 128)
                .handler(new LoggingHandler(LogLevel.DEBUG))
                .childHandler(channelInitializer);
        b.bind(address).sync();
        logger.info("NettyServer now listening on address {}", address);
    }
}

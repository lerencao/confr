package org.confr.server;

import org.confr.config.ConfServerConfig;
import org.confr.storage.ZkValueDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

/**
 * Conf Server
 */
class ConfServer {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private RequestHandlerPool requestHandlerPool;
    private NettyServer server;
    private ZkValueDispatcher dispatcher;
    private ConfServerConfig serverConfig;

    // TODO: add init: config step
    ConfServer(ConfServerConfig config) {
        this.serverConfig = config;
    }

    public void start() throws InstantiationException, IOException {
        dispatcher = new ZkValueDispatcher("localhost:2181", "confr");
        dispatcher.start();
        //        ConfRequests requestResponseHandler = new ConfRequests(dispatcher);

        server = new NettyServer(new InetSocketAddress(serverConfig.getBindAddress(), serverConfig.getBindPort()));
        server.start();

        NettyRequestResponseChannel requestResponseChannel = server.getRequestResponseChannel();
        ConfRequests confRequests = new ConfRequests(dispatcher, requestResponseChannel);
        requestHandlerPool = new RequestHandlerPool(2, requestResponseChannel, confRequests);
    }

    public void shutdown() {
        long startTime = System.currentTimeMillis();
        try {
            if (server != null) {
                server.shutdown();
            }

            if (requestHandlerPool != null) {
                requestHandlerPool.shutdown();
            }
        } catch (Exception e) {
            logger.error("Error when shutting down server", e);
        } finally {
            shutdownLatch.countDown();
            logger.info("Server shutdown in {} Ms.", System.currentTimeMillis() - startTime);
        }
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }

}

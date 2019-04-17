package org.confr.server;

import org.confr.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pool that will poll request from request channel, deliver it to other component to handle.
 */
public class RequestHandlerPool {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private Thread[] threads;
    private RequestHandler[] handlers;
    public RequestHandlerPool(int threadsNum, NettyRequestResponseChannel requestResponseChannel, ConfRequests requests) {
        assert threadsNum > 0;
        threads = new Thread[threadsNum];
        handlers = new RequestHandler[threadsNum];

        for (int i = 0; i < threadsNum; i++) {
            handlers[i] = new RequestHandler(i, requestResponseChannel, requests);
            threads[i] = ThreadUtils.newThread("request-handler-" + i, handlers[i], true);
            threads[i].start();
        }
    }

    public void shutdown() {
        try {
            logger.info("Shutting down");
            for (RequestHandler handler : handlers) {
                handler.shutdown();
            }

            for (Thread thread : threads) {
                thread.join();
            }
            logger.info("Shutdown completely");
        } catch (Exception e) {
            logger.error("Error when shutdown request handler pool {}", e);
        }
    }


}

class RequestHandler implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final int id;
    private final NettyRequestResponseChannel requestResponseChannel;
    private final ConfRequests requests;
    RequestHandler(int id, NettyRequestResponseChannel requestResponseChannel, ConfRequests requests) {
        this.id = id;
        this.requestResponseChannel = requestResponseChannel;
        this.requests = requests;
    }

    @Override public void run() {
        while (true) {
            try {
                ChannelCommand cmd = requestResponseChannel.receiveCommand();
                if (cmd.equals(ShutdownRequestHandlerCmd.INSTANCE)) {
                    logger.debug("Request handler {} receive shutdown signal", id);
                    break;
                } else if (cmd instanceof ClearSessionCmd) {
                    requests.clearSession(((ClearSessionCmd) cmd));
                } else if (cmd instanceof RequestInfo) {
                    // TODO: impl handle of req
                    requests.handleRequest(((RequestInfo) cmd));
                }

            } catch (Throwable e) {
                logger.error("Exception when handling request", e);
                Runtime.getRuntime().halt(1);
            }
        }
    }

    void shutdown() throws InterruptedException {
        requestResponseChannel.sendCommand(ShutdownRequestHandlerCmd.INSTANCE);
    }
}

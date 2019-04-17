package org.confr.server;

import org.confr.utils.AbstractServerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class ResponseHandler extends AbstractServerThread {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private NettyRequestResponseChannel requestResponseChannel;

    public ResponseHandler(NettyRequestResponseChannel requestResponseChannel) throws IOException{
        this.requestResponseChannel = requestResponseChannel;
    }

    @Override public void run() {
        startupComplete();
        logger.info("Startup complete");
        try {
            while (isRunning()) {
                processNewResponse();
            }
        } finally {
            try {
                shutdownComplete();
                logger.info("Shutdown complete");
                super.shutdown();
            } catch (InterruptedException err) {
                logger.error("InterruptedException when shutdwon", err);
            }
        }
    }



    private void processNewResponse() {
        ResponseInfo curr = requestResponseChannel.receiveResponse();
        while (curr != null) {
            try {
                if (curr.getData() == null) {
                    logger.trace("Process null response, closing channel {}", curr.getChannel().id());
                    curr.getChannel().close();
                } else if (curr.getChannel().isActive()) {
                    curr.getChannel().writeAndFlush(curr.getData());
                    logger.trace("Process {}, Write to channel {}", curr.getData().getType(), curr.getChannel().id());
                }
            } catch (Exception e) {
                logger.info("Error while process response, closing channel {}" + curr.getChannel().id(), e);
                curr.getChannel().close();
            } finally {
                curr = requestResponseChannel.receiveResponse();
            }
        }
    }
}

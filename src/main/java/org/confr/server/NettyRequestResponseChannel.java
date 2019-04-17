package org.confr.server;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * Bridge of request and request handling.
 */
public class NettyRequestResponseChannel {
    private final int queueCapacity;
    private final ArrayBlockingQueue<ChannelCommand> requestQueue;
    private final ArrayBlockingQueue<ResponseInfo> responseQueue;

    public NettyRequestResponseChannel(int queueCapacity) {
        this.queueCapacity = queueCapacity;
        this.requestQueue = new ArrayBlockingQueue<ChannelCommand>(this.queueCapacity);
        this.responseQueue = new ArrayBlockingQueue<ResponseInfo>(this.queueCapacity);
    }

    public void sendCommand(ChannelCommand command) throws InterruptedException {
        requestQueue.put(command);
    }
    public void sendResponse(ResponseInfo responseInfo) throws InterruptedException {
        responseQueue.put(responseInfo);
    }

    /** Get the next request or block until there is one */
    public ChannelCommand receiveCommand() throws InterruptedException {
        return requestQueue.take();
    }

    /** Get a response for the given processor if there is one, null if none */
    public ResponseInfo receiveResponse() {
        return responseQueue.poll();
    }

    public int getRequestQueueSize() {
        return requestQueue.size();
    }

    public int getResponseQueueSize() {
        return responseQueue.size();
    }

    public void shutdown() {
        requestQueue.clear();
    }

}


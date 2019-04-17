package org.confr.server;

import org.confr.messages.ConfrMessage;
import io.netty.channel.Channel;

/**
 * Define all the command involved in server handle circle.
 */
abstract class ChannelCommand {
    private final Channel channel;

    ChannelCommand(Channel channel) {
        this.channel = channel;
    }

    public Channel getChannel() {
        return channel;
    }
}

class RequestInfo<T extends ConfrMessage> extends ChannelCommand {
    private final T message;

    RequestInfo(Channel channel, T message) {
        super(channel);
        this.message = message;
    }

    T getMessage() {
        return message;
    }
}

class ResponseInfo<T extends ConfrMessage> extends ChannelCommand {
    private final T data;

    ResponseInfo(Channel channel, T data) {
        super(channel);
        this.data = data;
    }

    T getData() {
        return data;
    }
}

class ClearSessionCmd extends ChannelCommand {
    ClearSessionCmd(Channel channel) {
        super(channel);
    }
}


class ShutdownRequestHandlerCmd extends ChannelCommand {
    static final ShutdownRequestHandlerCmd INSTANCE = new ShutdownRequestHandlerCmd();
    private ShutdownRequestHandlerCmd() {
        super(null);
    }
}


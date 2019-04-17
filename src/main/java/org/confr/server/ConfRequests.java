package org.confr.server;

import io.netty.channel.Channel;
import io.netty.channel.ChannelId;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.confr.messages.ConfChangeResponse;
import org.confr.messages.ConfrMessage;
import org.confr.messages.Ping;
import org.confr.messages.Pong;
import org.confr.messages.WatchKeysRequest;
import org.confr.messages.WatchKeysResponse;
import org.confr.storage.ValueChangeListener;
import org.confr.storage.ValueChangedEvent;
import org.confr.storage.ZkValueDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle all request here.
 * Requests from same channel should be handled one after another to keep data in order.

 * Response are sent to this handler, queued if necessary.
 * TODO: It should ensure that responses to the same channel should be processed sequentially.
 */
public class ConfRequests {
    private static final Logger logger = LoggerFactory.getLogger(ConfRequests.class);

    private final ZkValueDispatcher dispatcher;
    private NettyRequestResponseChannel requestResponseChannel;
    private ConcurrentHashMap<ChannelId, Session> activeSessions = new ConcurrentHashMap<>();

    ConfRequests(ZkValueDispatcher dispatcher, NettyRequestResponseChannel requestResponseChannel) {
        this.dispatcher = dispatcher;
        this.requestResponseChannel = requestResponseChannel;
    }

    void clearSession(ClearSessionCmd cmd) {
        closeSession(cmd.getChannel());
    }

    void handleRequest(RequestInfo requestInfo) {
        Channel channel = requestInfo.getChannel();
        ConfrMessage requestMsg = requestInfo.getMessage();
        logger.trace("Handle request from channel {}", channel.id());
        if (!channel.isActive()) {
            logger.trace("Channel {} is inactive, close related session now", channel.id());
            closeSession(channel);
            return;
        } else if (requestMsg == null) {
            logger.trace("Request message of channel {} is null, close related session now", channel.id());
            closeSession(channel);
            return;
        }

        logger.trace("Start handle message {}", requestInfo.getMessage().getType());
        if (requestInfo.getMessage() instanceof Ping) {
            handlePing(((Ping) requestInfo.getMessage()), requestInfo.getChannel());
        } else if (requestInfo.getMessage() instanceof WatchKeysRequest) {
            handleWatch(((WatchKeysRequest) requestInfo.getMessage()), requestInfo.getChannel());
        }
    }

    private void handlePing(Ping ping, Channel channel) {
        ResponseInfo<Pong> pongResponseInfo = new ResponseInfo<>(channel,
                new Pong(ping.getVersionId(), ping.getCorrelationId(), ping.getClientId())
        );
        sendResponse(pongResponseInfo);
    }

    private void handleWatch(WatchKeysRequest request, Channel channel) {
        final Set<String> watchedKeys = request.getKeys();
        logger.trace("Watch request {} from channel {}, size: {}",
                request.getCorrelationId(), channel.id(), watchedKeys.size());
        // left this for session manager, the response will be generated and sent from session manager.
        // TODO(changyang): when to remove it?
        // When the channel is closed, notify the session manager.
        try {
            Session session = activeSessions.computeIfAbsent(channel.id(), channelId -> new Session(channel));
            // watch the underline storage
            HashMap<String, String> data = new HashMap<>();
            for (String key : watchedKeys) {
                String configValue = session.watch(key);
                data.put(key, configValue == null ? "" : configValue);
                logger.trace("Watch key {} for channel {} done, data: {}", key, channel.id(), data.get(key));
            }

            WatchKeysResponse response = WatchKeysResponse.builder().addConfigData(data)
                    .setClientId(request.getClientId())
                    .setCorrelationId(request.getCorrelationId())
                    .build();
            ResponseInfo<WatchKeysResponse> watchKeysResponseInfo = new ResponseInfo<>(channel, response);
            sendResponse(watchKeysResponseInfo);
        } catch (Exception e) {
            // TODO: handle failure
            e.printStackTrace();
        }
    }

    private void closeSession(Channel channel) {
        Session s = activeSessions.remove(channel.id());
        if (s != null) {
            try {
                s.close();
            } catch (IOException e) {
                logger.error("Error while close session of channel " + channel.id(), e);
            }
        }
    }


    private void sendResponse(ResponseInfo responseInfo) {
        try {
            requestResponseChannel.sendResponse(responseInfo);
        } catch (InterruptedException e) {
            responseInfo.getChannel().close();
        }
    }

    /**
     * Session abstracts the connection and config key interests from it,
     * may contain other into like auth info, if such thing exists.
     */
    class Session implements Closeable {
        //    private final String sessionId;
        private final Channel channel; // a connection from client
        private final HashMap<String, ValueChangeListener> listeners;
        private final Object lock = new Object();

        Session(Channel channel) {
            this.channel = channel;
            this.listeners = new HashMap<>(16);
        }

        public Channel getChannel() {
            return channel;
        }

        // NOTICE: Although cmds sent from the same channel may handled sequentially,
        // we still need to make sure watch and unwatch for the session should be executed sequentially.
        String watch(String key) throws Exception {
            synchronized (lock) {
                this.listeners.computeIfAbsent(key, k -> new SessionValueChangeListener(channel));
                return dispatcher.register(key, this.listeners.get(key));
            }
        }

        public void unwatch(String key) throws IOException {
            synchronized (lock) {
                ValueChangeListener listener = listeners.get(key);
                if (listener == null) {
                    // TODO: this case should not happen, add error log
                    return;
                }
                dispatcher.deregister(key, listener);
                listeners.remove(key);
            }
        }

        @Override public void close() throws IOException {
            synchronized (lock) {
                for (Map.Entry<String, ValueChangeListener> entry : listeners.entrySet()) {
                    dispatcher.deregister(entry.getKey(), entry.getValue());
                }

                listeners.clear();
            }
        }
    }

    class SessionValueChangeListener implements ValueChangeListener {
        private final Channel channel;
        SessionValueChangeListener(Channel channel) {
            this.channel = channel;
        }

        @Override public void onChanged(ValueChangedEvent event) {
            // When Not Exist, it mean the key node is not exist.
            // We assume a real delete never happen in zk.
            // Deleting is just a mark on the node.
            // TODO: handle the session id
            ConfChangeResponse response = ConfChangeResponse.builder().setClientId("fake session id")
                    .addConfigData(event.getKey(), event.getData() == null ? "" : event.getData()).build();

            ResponseInfo<ConfChangeResponse> confChangeResponseInfo =
                    new ResponseInfo<>(channel, response);

            sendResponse(confChangeResponseInfo);
        }
    }
}

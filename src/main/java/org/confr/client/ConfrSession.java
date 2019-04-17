package org.confr.client;

import io.netty.channel.Channel;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.confr.messages.ConfChangeResponse;
import org.confr.messages.ConfrMessage;
import org.confr.messages.Pong;
import org.confr.messages.WatchKeysRequest;
import org.confr.messages.WatchKeysResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Confr client session to manage a client session id and watched config,
 * and also the config cache.
 */
class ConfrSession implements ResponseHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConfrSession.class);

    private final String sessionId = UUID.randomUUID().toString().replace("-", "");
    private final AtomicLong requestIdGenerator = new AtomicLong(0);
    private final ConcurrentHashMap<String, String> configCache = new ConcurrentHashMap<>();
    private final Set<String> watchedConfig;
    private final ConcurrentHashMap<Long, CompletableFuture<Object>> inflightRequests = new ConcurrentHashMap<>();

    private NettyClient transport;

    ConfrSession(Set<String> watchedConfig) {
        this.watchedConfig = watchedConfig;
    }

    public ConfrSession withTransport(NettyClient transport) {
        this.transport = transport;
        this.transport.addReconnectListener(new ServerReconnectedListener() {
            @Override public void onConnected() {
                try {
                    rewatch();
                } catch (InterruptedException e) {
                    logger.error("Fail to rewatch config", e);
                }
            }
        });
        return this;
    }

    public ResponseHandler getResponseHandler() {
        return this;
    }

    Set<String> getWatchedConfig() {
        return watchedConfig;
    }

    public CompletableFuture<Void> watchConfig(String key) {
        String config = configCache.get(key);
        if (config != null) {
            CompletableFuture<Void> f = new CompletableFuture<Void>();
            f.complete(null);
            return f;
        }

        return watch(Collections.singleton(key));
    }

    public String getConfigCache(String key) {
        return configCache.get(key);
    }


    CompletableFuture<Void> watch(Set<String> keys) {
        WatchKeysRequest request = WatchKeysRequest.builder().setClientId(getSessionId())
                .setCorrelationId(nextRequestId()).addWatchKeys(keys).build();

        return doRequest(request).thenRun(() -> {});
    }

    private CompletableFuture<Void> rewatch() throws InterruptedException {
        return watch(watchedConfig);
    }

    private CompletableFuture<Object> doRequest(ConfrMessage request) {
        CompletableFuture<Object> f = new CompletableFuture<>();
        // f will be completed when remote server return a response with the same request id.
        addInflightRequest(request.getCorrelationId(), f);

        transport.send(request).addListener(future -> {
            if (!future.isSuccess()) {
                completeRequest(request.getCorrelationId(), future.cause());
            }
        });
        return f;
    }

    private String getSessionId() {
        return sessionId;
    }

    private long nextRequestId() {
        return requestIdGenerator.getAndIncrement();
    }

    private void addInflightRequest(long requestId, CompletableFuture<Object> f) {
        inflightRequests.putIfAbsent(requestId, f);
    }
    private void completeRequest(long requestId, Throwable e) {
        CompletableFuture<Object> future = inflightRequests.remove(requestId);
        if (future == null) {
            logger.warn("Request {} already completed", requestId);
        } else {
            future.completeExceptionally(e);
        }
    }

    @Override
    public void handleResponse(ConfrResponseInfo responseInfo) {
        ConfrMessage msg = responseInfo.getMessage();
        Channel responseChannel = responseInfo.getResponseChannel();
        logger.info("Receive msg {} from server {}", msg.getType(), responseChannel.remoteAddress());
        if (msg instanceof Pong) {
        } else if (msg instanceof WatchKeysResponse) {
            handleWatchResponse(((WatchKeysResponse) msg), responseInfo.getResponseChannel());
        } else if (msg instanceof ConfChangeResponse) {
            handleConfChange(((ConfChangeResponse) msg), responseInfo.getResponseChannel());
        }
    }

    private void handleConfChange(ConfChangeResponse msg, Channel responseChannel) {
        Map<String, String> changedConfig = msg.getData();
        changedConfig.forEach((k, v) -> {
            if (!watchedConfig.contains(k)) {
                logger.error("Client is not watching key {}, Please check server impl to fix this", k);
            } else {
                configCache.put(k, v);
            }
        });
    }

    private void handleWatchResponse(WatchKeysResponse response, Channel responseChannel) {
        String clientId = response.getClientId();
        long correlationId = response.getCorrelationId();
        if (!Objects.equals(clientId, this.sessionId)) {
            logger.error("Response client id {} is not equal to current session id {}, ignore the response", clientId, sessionId);
            return;
        }

        Map<String, String> configData = response.getConfigData();
        configData.forEach((key, value) -> {
            if (!watchedConfig.contains(key)) {
                logger.error("Client is not watching key {}, Please check server impl to fix this", key);
            } else {
                // TODO: based on config version to udpate the cache
                configCache.put(key, value);
            }
        });

        // Handle the request future
        CompletableFuture<Object> requestCompleteFuture = inflightRequests.remove(correlationId);
        if (requestCompleteFuture == null) {
            logger.error(
                    "the request {} has been removed from in flight request queue, Please check the impl to fix this",
                    correlationId);
        } else if (requestCompleteFuture.isDone()) {
            logger.warn("the request {} has been done", correlationId);
        } else {
            logger.trace("Complete request {}", correlationId);
            requestCompleteFuture.complete(configData);
        }
    }
}

class ConfrResponseInfo {
    private final ConfrMessage message;
    private final Channel responseChannel;

    public ConfrResponseInfo(ConfrMessage message, Channel responseChannel) {
        this.message = message;
        this.responseChannel = responseChannel;
    }

    ConfrMessage getMessage() {
        return message;
    }

    Channel getResponseChannel() {
        return responseChannel;
    }
}


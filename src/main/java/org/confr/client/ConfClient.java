package org.confr.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConfClient {
    private static final Logger logger = LoggerFactory.getLogger(ConfClient.class);
    private static final String[] Watched_Keys = new String[] {
            "testkey1",
            "testkey2"
    };
    public static void main(String[] args) throws InterruptedException {
        ConfClient client = ConfClient.builder().withServerAddress("0.0.0.0").withServerPort(8844)
                .withWatchConfig(Arrays.asList(Watched_Keys)).build();
        AtomicBoolean alive = new AtomicBoolean(true);
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal received, shutdown now...");
                alive.set(false);
            }));

            client.start();

            while (alive.get()) {
                for (String watched_key : Watched_Keys) {
                    logger.info("key: {}, value: {}", watched_key, client.getConfig(watched_key));
                }
                Thread.sleep(1000);
            }

            client.shutdown();
        } catch (InstantiationException e) {
            e.printStackTrace();
            logger.error("Error shutdown client", e);
        }

        logger.info("Shutdown finished, Bye!");
    }

    public static Builder builder() {
        return new Builder();
    }

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final NettyClient nettyClient;

    private final ConfrSession session;

    private ConfClient(SocketAddress address, Set<String> watchedConfig) {
        this.session = new ConfrSession(watchedConfig);
        this.nettyClient = new NettyClient(address, this.session.getResponseHandler());
        this.session.withTransport(this.nettyClient);
    }

    public void start() throws InstantiationException {
        this.nettyClient.start();
    }

    public void shutdown() {
        this.nettyClient.shutdown();
        this.shutdownLatch.countDown();
    }

    public void awaitShutdown() throws InterruptedException {
        this.shutdownLatch.await();
    }

    /**
     * watch the change of the key
     * @param key key to be watched
     * @return the current value of key if watch successfully
     * @throws Exception
     */
    public String watchConfig(String key) {
        session.watchConfig(key).join();
        return getConfig(key);
    }

    public String getConfig(String key) {
        return session.getConfigCache(key);
    }

    public static class Builder {
        private String confServerAddress;
        private int confServerPort;
        private Set<String> watchedConfig = new HashSet<>();

        Builder withServerAddress(String confServerAddress) {
            this.confServerAddress = confServerAddress;
            return this;
        }

        Builder withServerPort(int confServerPort) {
            this.confServerPort = confServerPort;
            return this;
        }

        Builder withWatchConfig(String key) {
            return this.withWatchConfig(Collections.singleton(key));
        }

        Builder withWatchConfig(Collection<String> keys) {
            watchedConfig.addAll(keys);
            return this;
        }

        ConfClient build() {
            return new ConfClient(new InetSocketAddress(confServerAddress, confServerPort), watchedConfig);
        }
    }
}

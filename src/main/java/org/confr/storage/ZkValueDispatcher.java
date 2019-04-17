package org.confr.storage;

import io.vavr.control.Try;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dispatcher zk data change to all kind of listeners
 * This class is thread safe.
 */
public class ZkValueDispatcher {
    private static final String Default_Namespace = "confr";
    private static final String Data_Path = "/data";
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final CuratorFramework client;
    private ConcurrentHashMap<String, ZkValueWatcher> watchers;

    public ZkValueDispatcher(String connectStr, String namespace) {
        this.client = CuratorFrameworkFactory.builder()
                .connectString(connectStr)
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        this.watchers = new ConcurrentHashMap<>(32);
    }

    public void start() {
        client.start();
    }

    public void shutdown() throws IOException {
        for (ZkValueWatcher watcher : this.watchers.values()) {
            watcher.close();
        }
        client.close();
    }

    /**
     * register a listener to watch key change. When first created, it will block for a while.
     * @param key the watched key
     * @param listener the listener callback when key changed
     * @return current key value, null if not exists
     * @throws Exception when failed to start node changer listener
     */
    public String register(String key, ValueChangeListener listener) throws Exception {
        // Use try to cover checked exception on watcher.register
        ZkValueWatcher watcher = watchers.compute(key, (k, w) -> Try.of(() -> {
            ZkValueWatcher newWatcher = w == null ? new ZkValueWatcher(client, k) : w;
            newWatcher.register(listener);
            return newWatcher;
        }).get());

        return new ValueChangedEventZkBuilder().withData(watcher.nodeCache.getCurrentData())
                .withKey(key)
                .build()
                .getData();
    }

    /**
     * deregister the listener of key from registery
     * @param key the watched key
     * @param listener the listener that passed when `register`
     * @throws IOException when io error
     */
    public void deregister(String key, ValueChangeListener listener) throws IOException {
        watchers.compute(key, (k, v) -> {
            if (v == null) {
                // error, this means bug exists
                logger.error("Deregister failed: nodeCache of key {} is not exists. There must be some bug!", k);
                return null;
            } else {
                // NOTICE: raise error if deregister failed.
                Try.run(() -> v.deregister(listener)).get();
                // if no listener exists, remove it from watcher list.
                if (v.listenerContainer.size() == 0) {
                    return null;
                } else {
                    return v;
                }
            }
        });
    }

    CuratorFramework getClient() {
        return client;
    }

    ConcurrentHashMap<String, ZkValueWatcher> getWatchers() {
        return watchers;
    }

    static String zkPathForKey(String key) {
        return ZKPaths.makePath(Data_Path, key);
    }

    static class ZkValueWatcher implements Closeable {
        private final NodeCache nodeCache;
        private final ListenerContainer<ValueChangeListener> listenerContainer;
        private final AtomicBoolean running = new AtomicBoolean(false);

        ZkValueWatcher(CuratorFramework client, String key) {
            nodeCache = new NodeCache(client, zkPathForKey(key));
            listenerContainer = new ListenerContainer<>();

            nodeCache.getListenable().addListener(
                    new ValueDispatcherNodeCacheListener(nodeCache, key, listenerContainer)
            );
        }

        void register(ValueChangeListener listener) throws Exception {
            synchronized (this) {
                ensureRunning();
                listenerContainer.addListener(listener);
            }
        }

        void deregister(ValueChangeListener listener) throws IOException {
            synchronized (this) {
                listenerContainer.removeListener(listener);
                if (listenerContainer.size() == 0) {
                    ensureStop();
                    listenerContainer.clear();
                }
            }
        }

        public ListenerContainer<ValueChangeListener> getListenerContainer() {
            return listenerContainer;
        }

        @Override public void close() throws IOException {
            synchronized (this) {
                ensureStop();
            }
        }

        private void ensureRunning() throws Exception {
            if (running.get()) {
                return;
            }
            running.set(true);
            nodeCache.start(true);
        }

        private void ensureStop() throws IOException {
            if (!running.get()) {
                return;
            }
            running.set(false);
            nodeCache.close();
        }
    }

}


class ValueDispatcherNodeCacheListener implements NodeCacheListener {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final NodeCache cache;
    private final String key;
    private final ListenerContainer<ValueChangeListener> container;
    ValueDispatcherNodeCacheListener(NodeCache cache, String key, ListenerContainer<ValueChangeListener> container) {
        this.cache = cache;
        this.key = key;
        this.container = container;
    }

    @Override public void nodeChanged() throws Exception {
        ChildData data = cache.getCurrentData();
        // TODO: handle the event version correctly
        final ValueChangedEvent event = new ValueChangedEventZkBuilder().withKey(key).withData(data).build();

        container.forEach(listener -> {
            try {
                if (listener != null) {
                    listener.onChanged(event);
                }
            } catch (Exception e) {
                logger.error("Call ValueChangeListener failed.", e);
            }
            return null;
        });
    }
}

class ValueChangedEventZkBuilder {
    private String key;
    private ChildData data;
    ValueChangedEventZkBuilder withKey(String key) {
        this.key = key;
        return this;
    }

    ValueChangedEventZkBuilder withData(ChildData data) {
        this.data = data;
        return this;
    }

    ValueChangedEvent build() {
        if (data == null) {
            return ValueChangedEvent.NotExist(key);
        } else {
            return ValueChangedEvent.Updated(key, data.getStat().getVersion(), new String(data.getData()));
        }
    }
}

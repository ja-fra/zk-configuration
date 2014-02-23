package io.redlink.zookeeper.configuration;

import com.google.common.cache.*;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

public class ZookeeperConfiguration extends AbstractConfiguration {

    private final String zkConnectionString;
    private final int zkTimeout;
    private final LoadingCache<String, Object> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Object>() {
                @Override
                public Object load(String key) throws Exception {
                    return zk.getData(key, true, null);
                }
            });

    private ZooKeeper zk = null;

    public ZookeeperConfiguration(String zkConnectionString) throws IOException {
        this.zkConnectionString = zkConnectionString;
        this.zkTimeout = 5000;
    }

    private void zkInit() throws IOException {
        // if (zk != null) zk.close();
        zk = new ZooKeeper(zkConnectionString, 5000, new ZKWatcher());
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        // FIXME
        zk.create(key, value.toString().getBytes(), null, CreateMode.PERSISTENT);

    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(String key) {
        return cache.get(key) != null;
    }

    @Override
    public Object getProperty(String key) {
        return cache.get(key);
    }

    @Override
    public Iterator<String> getKeys() {
        return null;
    }

    private class ZKWatcher implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {
            switch (watchedEvent.getState()) {
                case Disconnected:
                case Expired:
                    cache.invalidateAll();
                    zkInit();
                    break;
            }
            switch (watchedEvent.getType()) {
                case NodeDataChanged:
                case NodeDeleted:
                    cache.invalidate(watchedEvent.getPath());
                    break;
                case None:
            }

        }
    }
}

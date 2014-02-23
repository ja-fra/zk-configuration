package io.redlink.zookeeper.configuration;

import com.google.common.cache.*;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ZookeeperConfiguration extends AbstractConfiguration {

    private static final Logger log = LoggerFactory.getLogger(ZookeeperConfiguration.class);

    private final String zkConnectionString;
    private final int zkTimeout;
    private final String zkRoot;

    private final LoadingCache<String, String> cache = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<Object, Object>() {
                @Override
                public void onRemoval(RemovalNotification<Object, Object> notification) {
                    LoggerFactory.getLogger(this.getClass()).debug("Cache entry removed: {} ({})", notification.getKey(), notification.getCause().toString());
                }
            })
            .build(new CacheLoader<String, String>() {
                private final Logger log = LoggerFactory.getLogger(this.getClass());

                @Override
                public String load(String key) throws Exception {
                    log.trace("loading Key: {}", key);
                    try {
                        return new String(zk.getData(zkRoot + key, true, null));
                    } catch (final Throwable t) {
                        log.error("Cache.load: " + t.getMessage(), t);
                        throw t;
                    }
                }
            });
    private List<String> keyList;
    private ZooKeeper zk = null;

    public ZookeeperConfiguration(String zkConnectionString) throws IOException {
        this.zkConnectionString = zkConnectionString;
        this.zkTimeout = 5000;
        this.zkRoot = "/";

        zkInit();
    }

    private void zkInit() throws IOException {
        final CountDownLatch sync = new CountDownLatch(1);
        log.debug("zkInit - connecting");
        // if (zk != null) zk.close();
        zk = new ZooKeeper(zkConnectionString, zkTimeout, new ZKWatcher(sync));

        try {
            sync.await(zkTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new IOException("Could not connect", e);
        }
        log.info("zkInit - connected");
    }

    @Override
    protected void clearPropertyDirect(String key) {
        try {
            zk.delete(zkRoot + key, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }



    @Override
    protected void addPropertyDirect(String key, Object value) {
        final String path = zkRoot + key;
        // FIXME: this is no add but set!
        try {
            final Stat stat = zk.exists(path, false);
            if (stat != null) {
                log.debug("{} already exists, overwrite", key);
                zk.setData(path, value.toString().getBytes(), stat.getVersion());
            } else {
                log.debug("new key {}", key);
                zk.create(path, value.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean isEmpty() {
        return listKeys().isEmpty();
    }

    @Override
    public boolean containsKey(String key) {
        return listKeys().contains(key);
    }

    @Override
    public Object getProperty(String key) {
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Iterator<String> getKeys() {
        return listKeys().iterator();
    }

    private List<String> listKeys() {
        try {
            // FIXME: Concurrency?
            if (keyList == null) {
                keyList = zk.getChildren(zkRoot, true);
            }
            return keyList;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    private class ZKWatcher implements Watcher {

        private final Logger log = LoggerFactory.getLogger(ZKWatcher.class);
        private final CountDownLatch syncConnect;

        public ZKWatcher(CountDownLatch syncConnect) {
            this.syncConnect = syncConnect;
        }

        @Override
        public void process(WatchedEvent watchedEvent) {
            if (log.isTraceEnabled()) {
                log.trace("zk Event: {}", watchedEvent);
            }

            switch (watchedEvent.getType()) {
                case None:
                    break;
                case NodeDeleted:
                case NodeDataChanged:
                    cache.invalidate(watchedEvent.getPath().substring(zkRoot.length()));
                    return;
                case NodeCreated:
                    // nop;
                    break;
                case NodeChildrenChanged:
                    keyList = null;
                    break;
            }

            switch (watchedEvent.getState()) {
                case SyncConnected:
                    log.trace("zk connected");
                    syncConnect.countDown();
                    break;
                case Expired:
                case Disconnected:
                    log.info("zk {}, trying to reconnect", watchedEvent.getState());
                    cache.invalidateAll();
                    try {
                        zk.close();
                    } catch (InterruptedException e) {
                        log.warn("exception while closing connection", e);
                    }
                    try {
                        zkInit();
                    } catch (IOException e) {
                        throw new IllegalStateException("zk Reconnect failed", e);
                    }
                    break;
                case AuthFailed:
                    log.error("zk auth failed");
                    throw new IllegalStateException("zk Authentication failed");
                case ConnectedReadOnly:
                    log.warn("zk read only");
                    break;
                case SaslAuthenticated:
                    log.info("zk authenticated");
                    break;
            }
        }
    }
}

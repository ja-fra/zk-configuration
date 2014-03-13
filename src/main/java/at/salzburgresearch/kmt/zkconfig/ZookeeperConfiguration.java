/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.salzburgresearch.kmt.zkconfig;

import com.google.common.cache.*;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.PropertyConverter;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
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

    private final LoadingCache<String, Object> cache = CacheBuilder.newBuilder()
            .concurrencyLevel(4)
            .removalListener(new RemovalListener<Object, Object>() {
                @Override
                public void onRemoval(RemovalNotification<Object, Object> notification) {
                    log.debug("Cache entry removed: {} ({})", notification.getKey(), notification.getCause().toString());
                }
            })
            .build(new CacheLoader<String, Object>() {
                @Override
                public Object load(String key) throws Exception {
                    log.trace("loading Key: {}", key);
                    try {
                        sync.await();
                        return deserialize(zk.getData(key, true, null));
                    } catch (KeeperException e) {
                        switch (e.code()) {
                            case NONODE:
                                // this is expected if the requested node does not exist.
                                throw e;
                            case CONNECTIONLOSS:
                            case SESSIONEXPIRED:
                                // reconnect & try again
                                sync.raiseBarrier();
                                return this.load(key);
                            default:
                                log.error("Cache.load: " + e.getMessage(), e);
                                throw e;
                        }
                    } catch (final Throwable t) {
                        log.error("Cache.load: " + t.getMessage(), t);
                        throw t;
                    }
                }
            });
    private final List<String> keyList = new ArrayList<>();

    private ZooKeeper zk = null;
    private final ConnectionBarrier sync;

    public ZookeeperConfiguration(String zkConnectionString, int zkTimeout, String zkRoot) throws IOException {
        this.zkConnectionString = zkConnectionString;
        this.zkTimeout = zkTimeout;
        // make sure the root starts and ends with a slash
        this.zkRoot = zkRoot.replaceFirst("^/?", "/").replaceFirst("/?$", "/");
        this.sync = new ConnectionBarrier();

        zkInit();
    }

    private void zkInit() throws IOException {
        sync.raiseBarrier();
        final CountDownLatch connected = new CountDownLatch(1);
        log.debug("zkInit - connecting");
        // if (zk != null) zk.close();
        zk = new ZooKeeper(zkConnectionString, zkTimeout, new ZKWatcher(connected, sync));

        log.info("zkInit - ensure root node exists");
        try {
            if (connected.await(zkTimeout, TimeUnit.MILLISECONDS)) {
                for (int i = zkRoot.indexOf('/', 1); i > 0; i = zkRoot.indexOf('/', i+1)) {
                    final String path = zkRoot.substring(0, i);
                    log.trace("zkInit - checking existence of {}", path);
                    if (zk.exists(path, false) == null) {
                        zk.create(path, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                }
                log.debug("zkInit - zkRoot {} exists", zkRoot);
            } else {
                throw new IOException("Timeout while establishing ZooKeeper connection");
            }
        } catch (InterruptedException e) {
            throw new IOException("Could not connect", e);
        } catch (KeeperException e) {
            throw new IOException("Initial Connection failed - is zookeeper available?", e);
        }
        log.info("zkInit - connected");
        sync.lowerBarrier();
    }

    @Override
    protected void clearPropertyDirect(String key) {
        try {
            final String path = toZookeeperPath(key);
            sync.await();
            zk.delete(path, -1);
            cache.invalidate(path);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            if (handleException(e)) {
                clearPropertyDirect(key);
                return;
            }
            e.printStackTrace();
        }
    }

    @Override
    protected void addPropertyDirect(String key, Object value) {
        throw new UnsupportedOperationException("addPropertyDirect should not be used");
    }

    @Override
    public void addProperty(String key, Object value) {
        fireEvent(EVENT_ADD_PROPERTY, key, value, true);

        final Object prevValue = getProperty(key);
        if (prevValue == null) {
            setPropertyDirect(key, value);
        } else {
            final List<Object> list;
            if (prevValue instanceof List) {
                @SuppressWarnings("unchecked")
                final List<Object> _l = (List<Object>) prevValue;
                list = _l;
            } else {
                list = new ArrayList<>();
                list.add(prevValue);
            }

            Iterator<?> it = PropertyConverter.toIterator(value, isDelimiterParsingDisabled() ? '\0' : getListDelimiter());
            while (it.hasNext()) {
                list.add(it.next());
            }
            setPropertyDirect(key, list);
        }

        fireEvent(EVENT_ADD_PROPERTY, key, value, false);
    }

    @Override
    public void setProperty(String key, Object value) {
        fireEvent(EVENT_SET_PROPERTY, key, value, true);
        setPropertyDirect(key, value);
        fireEvent(EVENT_SET_PROPERTY, key, value, false);
    }

    protected void setPropertyDirect(String key, Object value) {
        if (value == null) {
            clearPropertyDirect(key);
        } else {
            final String path = toZookeeperPath(key);
            try {
                sync.await();
                final Stat stat = zk.exists(path, false);
                if (stat != null) {
                    log.debug("{} already exists, overwrite", key);
                    zk.setData(path, serialize(value), stat.getVersion());
                } else {
                    log.debug("new key {}", key);
                    zk.create(path, serialize(value), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                if (handleException(e)) {
                    setPropertyDirect(key, value);
                    return;
                }
                e.printStackTrace();
            } finally {
                cache.invalidate(path);
            }
        }
    }

    private byte[] serialize(Object value) {
        try {
            if (value.getClass().isArray()) {
                StringBuilder sb = new StringBuilder(255);
                sb.append("(\"");
                Object[] value1 = (Object[]) value;
                for (int i = 0; i < value1.length; i++) {
                    if (i > 0) {
                        sb.append("\", \"");
                    }
                    sb.append(String.valueOf(value1[i]).replaceAll("\"", "\\\""));
                }
                sb.append("\")");
                return sb.toString().getBytes("utf8");
            } else if (value instanceof List) {
                StringBuilder sb = new StringBuilder(255);
                sb.append("(\"");
                final Iterator it = ((List) value).iterator();
                while (it.hasNext()) {
                    sb.append(String.valueOf(it.next()).replaceAll("\"", "\\\""));
                    if (it.hasNext()) {
                        sb.append("\", \"");
                    }
                }
                sb.append("\")");
                return sb.toString().getBytes("utf8");
            } else {
                return String.valueOf(value).getBytes("utf8");
            }
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("utf8 not supported, that's weird");
        }
    }

    private Object deserialize(byte[] data) {
        try {
            String string = new String(data, "utf8").trim();
            if (string.startsWith("(\"")) {
                List<Object> list = new ArrayList<>();
                string = string.substring(2, string.length()-2);
                for(String v:string.split("\"\\s*,\\s*\"")) {
                    list.add(v.replaceAll("\\\\\"", "\""));
                }
                return list;
            } else {
                return string;
            }
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException("utf8 not supported, that's weird");
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
            return cache.get(toZookeeperPath(key));
        } catch (ExecutionException e) {
            if (e.getCause() instanceof KeeperException && ((KeeperException) e.getCause()).code() == KeeperException.Code.NONODE) {
                return null;
            }
            log.error("Could not load property {}: {}", key, e);
        }
        return null;
    }

    @Override
    public Iterator<String> getKeys() {
        return listKeys().iterator();
    }

    private List<String> listKeys() {
        try {
            if (keyList.isEmpty()) {
                sync.await();
                synchronized (keyList) {
                    if (keyList.isEmpty()) {
                        keyList.addAll(zk.getChildren(zkRoot, true));
                    }
                }
            }
            return keyList;
        } catch (KeeperException e) {
            if (handleException(e)) {
                return listKeys();
            }
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    private String toZookeeperPath(String key) {
        return zkRoot + key.replaceAll("[/ ]", ".");
    }

    private boolean handleException(KeeperException e) {
        switch (e.code()) {
            case CONNECTIONLOSS:
            case SESSIONEXPIRED:
                this.sync.raiseBarrier();
                return true;
        }
        return false;
    }

    /**
     * for unit-testing only
     */
    long getZkSessionId() {
        return zk.getSessionId();
    }

    private class ZKWatcher implements Watcher {

        private final Logger log = LoggerFactory.getLogger(ZKWatcher.class);
        private final CountDownLatch syncConnect;
        private final ConnectionBarrier connectionBarrier;

        public ZKWatcher(CountDownLatch connected, ConnectionBarrier sync) {
            this.syncConnect = connected;
            this.connectionBarrier = sync;
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
                    cache.invalidate(watchedEvent.getPath());
                    return;
                case NodeCreated:
                    // nop;
                    break;
                case NodeChildrenChanged:
                    if (watchedEvent.getPath().equals(zkRoot)) {
                        if (!keyList.isEmpty()) {
                            synchronized (keyList) {
                                keyList.clear();
                            }
                        }
                    }
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
                    connectionBarrier.raiseBarrier();
                    cache.invalidateAll();
                    keyList.clear();
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

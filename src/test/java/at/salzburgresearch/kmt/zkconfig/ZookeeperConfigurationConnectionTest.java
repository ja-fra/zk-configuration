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

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.*;

public class ZookeeperConfigurationConnectionTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private ZooKeeperServer server;
    private String zkConnection;

    @Before
    public void setUp() throws Exception {
        int tickTime = 2000;
        int numConnections = 100;
        File dir = temp.newFolder("zkHome");

        server = new ZooKeeperServer(dir, dir, tickTime);
        ServerCnxnFactory serverFactory = ServerCnxnFactory.createFactory(0,numConnections);

        serverFactory.startup(server);

        zkConnection = "localhost:"+server.getClientPort();
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    public void testBasicCRUD() throws IOException {
        ZookeeperConfiguration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final String v1 = UUID.randomUUID().toString(),
                v2 = UUID.randomUUID().toString(),
                v3 = UUID.randomUUID().toString(),
                v4 = UUID.randomUUID().toString(),
                v5 = UUID.randomUUID().toString();


        assertNull(config.getString(key));
        assertEquals(v5, config.getString(key, v5));

        config.setProperty(key, v1);
        assertEquals(v1, config.getString(key, v5));

        server.closeSession(config.getZkSessionId());

        config.addProperty(key, v2);
        assertEquals(v1, config.getString(key, v5));
        assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v1, v2));

        config.addProperty(key, v3);
        server.closeSession(config.getZkSessionId());
        config.addProperty(key, v4);
        server.closeSession(config.getZkSessionId());
        assertEquals(v1, config.getString(key, v5));
        assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v1, v2, v3, v4));

        server.closeSession(config.getZkSessionId());
        config.clearProperty(key);
        assertNull(config.getString(key));
        assertEquals(v5, config.getString(key, v5));

        config.addProperty(key, v5);
        assertEquals(v5, config.getString(key));

        config.clearProperty(key);
        config.setProperty(key, Arrays.asList(v3, v2, v4, v1, v5));
        assertEquals(v3, config.getString(key));
        assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v3, v2, v4, v1, v5));

    }

    @Test(expected = IOException.class)
    public void testConnectionNotAvailable() throws Exception {
        server.shutdown();
        server = null;

        new ZookeeperConfiguration(zkConnection, 5000, "/test");
        fail("Must not connect to a non-available ZooKeeper!");
    }
}

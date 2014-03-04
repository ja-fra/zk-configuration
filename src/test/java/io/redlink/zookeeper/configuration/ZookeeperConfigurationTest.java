/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.redlink.zookeeper.configuration;

import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.hamcrest.CoreMatchers;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Basic Tests for ZookeeperConfiguration
 */
public class ZookeeperConfigurationTest {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private ZooKeeperServer server;
    private ServerCnxnFactory standaloneServerFactory;
    private String zkConnection;

    @Before
    public void setUp() throws Exception {
        ServerConfig config = new ServerConfig();

        int tickTime = 2000;
        int numConnections = 100;
        File dir = temp.newFolder("zkHome");

        server = new ZooKeeperServer(dir, dir, tickTime);
        standaloneServerFactory = ServerCnxnFactory.createFactory(0, numConnections);

        int zkPort = standaloneServerFactory.getLocalPort();

        standaloneServerFactory.startup(server);
        zkConnection = "localhost:"+zkPort;
    }

    @After
    public void tearDown() throws Exception {
        if (server != null) {
            server.shutdown();
        }
    }

    @Test
    public void testBasicCRUD() throws IOException {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final String v1 = UUID.randomUUID().toString(),
                v2 = UUID.randomUUID().toString(),
                v3 = UUID.randomUUID().toString(),
                v4 = UUID.randomUUID().toString(),
                v5 = UUID.randomUUID().toString();


        Assert.assertNull(config.getString(key));
        Assert.assertEquals(v5, config.getString(key, v5));

        config.setProperty(key, v1);
        Assert.assertEquals(v1, config.getString(key, v5));

        config.addProperty(key, v2);
        Assert.assertEquals(v1, config.getString(key, v5));
        Assert.assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v1, v2));

        config.addProperty(key, v3);
        config.addProperty(key, v4);
        Assert.assertEquals(v1, config.getString(key, v5));
        Assert.assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v1, v2, v3, v4));

        config.clearProperty(key);
        Assert.assertNull(config.getString(key));
        Assert.assertEquals(v5, config.getString(key, v5));

        config.addProperty(key, v5);
        Assert.assertEquals(v5, config.getString(key));

        config.clearProperty(key);
        config.setProperty(key, Arrays.asList(v3,v2,v4,v1,v5));
        Assert.assertEquals(v3, config.getString(key));
        Assert.assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v3, v2, v4, v1, v5));

    }
}

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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.configuration.Configuration;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.hamcrest.CoreMatchers;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.collection.IsArrayContainingInOrder.arrayContaining;
import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.*;

/**
 * Basic Tests for ZookeeperConfiguration
 */
public class ZookeeperConfigurationTest {

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
        ServerCnxnFactory serverFactory = ServerCnxnFactory.createFactory(0, numConnections);

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
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

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

        config.addProperty(key, v2);
        assertEquals(v1, config.getString(key, v5));
        assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v1, v2));

        config.addProperty(key, v3);
        config.addProperty(key, v4);
        assertEquals(v1, config.getString(key, v5));
        assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v1, v2, v3, v4));

        config.clearProperty(key);
        assertNull(config.getString(key));
        assertEquals(v5, config.getString(key, v5));

        config.addProperty(key, v5);
        assertEquals(v5, config.getString(key));

        config.clearProperty(key);
        config.setProperty(key, Arrays.asList(v3,v2,v4,v1,v5));
        assertEquals(v3, config.getString(key));
        assertThat(config.getList(key), CoreMatchers.<Object>hasItems(v3, v2, v4, v1, v5));

    }

    @Test
    public void testCuncurrent() throws IOException {
        Configuration con1 = new ZookeeperConfiguration(zkConnection, 5000, "/concurrent/");
        Configuration con2 = new ZookeeperConfiguration(zkConnection, 5000, "/concurrent/");

        final String key = UUID.randomUUID().toString();
        final String v1 = UUID.randomUUID().toString(),
                v2 = UUID.randomUUID().toString(),
                v3 = UUID.randomUUID().toString(),
                v4 = UUID.randomUUID().toString(),
                v5 = UUID.randomUUID().toString();


        assertNull(con1.getString(key));
        assertNull(con2.getString(key));
        assertEquals(v5, con1.getString(key, v5));
        assertEquals(v5, con2.getString(key, v5));

        con1.setProperty(key, v1);
        assertEquals(v1, con2.getString(key, v5));
        assertEquals(v1, con1.getString(key, v5));

        con2.addProperty(key, v2);
        assertEquals(v1, con1.getString(key, v5));
        assertEquals(v1, con2.getString(key, v5));
        assertThat(con2.getList(key), CoreMatchers.<Object>hasItems(v1, v2));
        assertThat(con1.getList(key), CoreMatchers.<Object>hasItems(v1, v2));

        con2.addProperty(key, v3);
        con1.addProperty(key, v4);
        assertEquals(v1, con2.getString(key, v5));
        assertEquals(v1, con1.getString(key, v5));
        assertThat(con2.getList(key), CoreMatchers.<Object>hasItems(v1, v2, v3, v4));
        assertThat(con1.getList(key), CoreMatchers.<Object>hasItems(v1, v2, v3, v4));

        con2.clearProperty(key);
        assertNull(con2.getString(key));
        assertNull(con1.getString(key));
        assertEquals(v5, con1.getString(key, v5));
        assertEquals(v5, con2.getString(key, v5));

        con1.addProperty(key, v5);
        assertEquals(v5, con2.getString(key));

        con1.clearProperty(key);
        con2.setProperty(key, Arrays.asList(v3,v2,v4,v1,v5));
        assertEquals(v3, con1.getString(key));
        assertThat(con2.getList(key), CoreMatchers.<Object>hasItems(v3, v2, v4, v1, v5));

    }

    @Test
    public void testString() throws Exception {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final String val1 = "This is a test value with some spëcial Ȼȃᵲḁçʈᴇřƨ " +
                "- to check serialization, deserialisation and encoding.",
                val2 = "An extended List, with several, commas, that should stay within, the, same value,";


        assertThat(config.getString(key), nullValue(String.class));

        config.setProperty(key, val1);
        assertThat(config.getString(key), equalTo(val1));
        assertThat(config.getList(key), hasSize(1));

        config.setProperty(key, val2);
        assertThat(config.getString(key), equalTo(val2));
        assertThat(config.getList(key), hasSize(1));
    }

    @Test
    public void testList() throws Exception {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final String val = "An extended List, with several, commas, that should stay within, the, same value,";
        final List<?> list = Lists.transform(Arrays.asList(val.split(",")), new Function<String, String>() {
            @Override
            public String apply(String input) {
                return input.trim();
            }
        });

        assertThat(config.getProperty(key), nullValue());

        config.setProperty(key, list);
        assertThat(config.getList(key), IsIterableContainingInOrder.contains(list.toArray()));
        assertThat(config.getString(key), is(val.split(",")[0].trim()));

        config.setProperty(key, val.split(","));
        assertThat(config.getString(key), is(val.split(",")[0]));
        assertThat(config.getList(key), CoreMatchers.<Object>hasItems(val.split(",")));
        assertThat(config.getStringArray(key), arrayContaining(val.split(",")));
        assertThat(config.getStringArray(key), arrayWithSize(val.split(",").length));
    }

    @Test
    public void testInt() throws Exception {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final Random random = new Random();
        final int val1 = random.nextInt();
        final Integer val2 = random.nextInt();

        assertThat(config.getProperty(key), nullValue());

        config.setProperty(key, val1);
        assertEquals(val1, config.getInt(key));
        assertEquals(new Integer(val1), config.getInteger(key, val2));

        config.setProperty(key, val2);
        assertEquals(val2.intValue(), config.getInt(key));
        assertEquals(val2, config.getInteger(key, val1));
    }

    @Test
    public void testFloat() throws Exception {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final Random random = new Random();
        final float val1 = random.nextFloat();
        final Float val2 = random.nextFloat();

        assertThat(config.getProperty(key), nullValue());

        config.setProperty(key, val1);
        assertEquals(val1, config.getFloat(key), 10e-6);
        assertEquals(new Float(val1), config.getFloat(key, val2));

        config.setProperty(key, val2);
        assertEquals(val2, config.getFloat(key), 10e-6);
        assertEquals(val2, config.getFloat(key, Float.valueOf(val1)));

    }

    @Test
    public void testDouble() throws Exception {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final Random random = new Random();
        final double val1 = random.nextDouble();
        final Double val2 = random.nextDouble();

        assertThat(config.getProperty(key), nullValue());

        config.setProperty(key, val1);
        assertEquals(val1, config.getDouble(key), 10e-6);
        assertEquals(Double.valueOf(val1), config.getDouble(key, val2));

        config.setProperty(key, val2);
        assertEquals(val2, config.getDouble(key), 10e-6);
        assertEquals(val2, config.getDouble(key, Double.valueOf(val1)));
    }

    @Test
    public void testBoolean() throws Exception {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();

        assertThat(config.getProperty(key), nullValue());

        config.setProperty(key, true);
        assertTrue(config.getBoolean(key, false));
        assertEquals(config.getBoolean(key, Boolean.FALSE), Boolean.TRUE);

        config.setProperty(key, false);
        assertFalse(config.getBoolean(key, true));
        assertEquals(config.getBoolean(key, Boolean.TRUE), Boolean.FALSE);

    }

    @Test
    public void testLong() throws Exception {
        Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

        final String key = UUID.randomUUID().toString();
        final Random random = new Random();
        final long val1 = random.nextLong();
        final Long val2 = random.nextLong();

        assertThat(config.getProperty(key), nullValue());

        config.setProperty(key, val1);
        assertEquals(val1, config.getLong(key));
        assertEquals(Long.valueOf(val1), config.getLong(key, val2));

        config.setProperty(key, val2);
        assertEquals(val2.longValue(), config.getLong(key));
        assertEquals(val2, config.getLong(key, Long.valueOf(val1)));
    }
}

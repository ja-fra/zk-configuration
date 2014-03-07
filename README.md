# ZookeeperConfiguration

ZookeeperConfiguration provides access to a configuration settings stored in a [ZooKeeper][1] ensemble via the [Commons Configuration][2] API.

## Usage:

```java
String zkConnection = "localhost:2181"; // ZooKeeper connection string
                                        // use normal zk connection string here
int zkTimeout = 5000; // timeout
                      // used for zkSessionTimeout and initial connectionTimeout
String zkRoot = "/config"; // path where to read/store the configuration
                           // will be (recursively) created as needed.

// Constructor will block until connection is established
Configuration config = new ZookeeperConfiguration(zkConnection, 5000, "/test");

// Use the Configuration object like any other Commons-Configuration implementation.
String val = config.getString(key);

config.setProperty(key, "Foo");

config.addProperty(key, "Bar");

List<?> vals = config.getList(key);

config.clearProperty(key);

```

[1]: http://zookeeper.apache.org/
[2]: http://commons.apache.org/proper/commons-configuration/


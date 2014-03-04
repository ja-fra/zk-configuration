package io.redlink.zookeeper;

import io.redlink.zookeeper.configuration.ZookeeperConfiguration;

import java.io.IOException;

/**
 * Created by jakob on 2/23/14.
 */
public class Main {

    public static void main(String... args) {
        String zkConnect = "localhost:2181";

        try {
            System.out.println("Main: Start");
            ZookeeperConfiguration zkc = new ZookeeperConfiguration(zkConnect, 5000, "/test");

            System.out.println("Main: Set test=Foo");
            zkc.setProperty("test", "foo");
            System.out.println("Main: test=Foo");

            for (int i = 0; i < 10; i++) {
                System.out.println("Main: test=" + zkc.getString("test"));
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    System.out.println("interrupt");
                }
            }

            System.out.println("End");


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}

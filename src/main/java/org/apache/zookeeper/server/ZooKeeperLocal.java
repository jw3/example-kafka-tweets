package org.apache.zookeeper.server;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Local embedded Kafka. Taken from https://gist.github.com/fjavieralba/7930018
 * This has to be in org.apache.zookeeper.server package in order to access protected .shutdown() method.
 */
public class ZooKeeperLocal {

    ZooKeeperServerMain zooKeeperServer;

    public ZooKeeperLocal(Properties zkProperties) throws FileNotFoundException, IOException{
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);


        new Thread() {
            public void run() {
                try {
                    zooKeeperServer.runFromConfig(configuration);
                } catch (IOException e) {
                    System.out.println("ZooKeeper Failed");
                    e.printStackTrace(System.err);
                }
            }
        }.start();
    }

    public void shutdown() {
        zooKeeperServer.shutdown();
    }
}
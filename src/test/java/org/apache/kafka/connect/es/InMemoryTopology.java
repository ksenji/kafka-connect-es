package org.apache.kafka.connect.es;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.connect.es.dcl.Factory;
import org.apache.kafka.connect.es.dcl.Factory.Dcl;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import scala.Option;

public class InMemoryTopology {

    private Dcl<TestingServer> zkTestServer = Factory.of(() -> {
        TestingServer ts = null;
        try {
            ts = new TestingServer(2181, true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ts;
    });

    private Dcl<KafkaServerStartable> broker = Factory.of(() -> {
        Properties props = TestUtils.createBrokerConfig(1, zkTestServer.get().getConnectString(), true, false, 9092, Option.empty(), Option.empty(), true, false, 0, false, 0, false, 0);
        KafkaServerStartable kss = new KafkaServerStartable(new KafkaConfig(props));
        kss.startup();

        return kss;
    });

    private Dcl<Node> es = Factory.of(() -> {
        Settings settings = Settings.settingsBuilder()
                                    .put("http.enabled", "true")
                                    .put("node.master", "true")
                                    .put("http.port", 9201)
                                    .put("transport.tcp.port", 9301)
                                    .put("path.data", "/tmp/es/data")
                                    .put("path.home", "/tmp/es")
                                    .build();

        return NodeBuilder.nodeBuilder().client(false).clusterName("elasticsearch").data(true).settings(settings).build().start();
    });

    public TestingServer getTestingServer() {
        return zkTestServer.get();
    }

    public KafkaServerStartable getBroker() {
        return broker.get();
    }

    public Node getEsNode() {
        return es.get();
    }
    
    public void shutdown() {
        try {
            FileUtils.deleteDirectory(new File("/tmp/es/data"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        es.get().client().close();
        es.get().close();
        broker.get().shutdown();
        try {
            zkTestServer.get().close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

package org.apache.kafka.connect.es;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;
import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.cli.ConnectStandalone;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsAction;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSearchSinkConnectorIntegrationTest {

    private InMemoryTopology topology = new InMemoryTopology();
    private ExecutorService service = Executors.newFixedThreadPool(1);

//    private TestingServer ts;
//    private KafkaServerStartable broker;
    private Node esNode;
    private String topic = "person";

    @Before
    public void setUp() {
//        ts = topology.getTestingServer();
//        broker = topology.getBroker();
        esNode = topology.getEsNode();
    }

    @Test
    public void testKafkaConnectEs() {
        produce();
        service.submit(() -> {
            consumeViaKafkaConnectToEs();
        });

        Client client = esNode.client();
        waitUntilTypesAreCreatedInEs();

        //@formatter:off
        SearchResponse response = client.prepareSearch("person-index")
                                        .setTypes("person").setQuery(QueryBuilders.termQuery("firstName", "eric"))
                                        .setSize(0)
                                        .execute()
                                        .actionGet();
        //@formatter:on

        long hits = response.getHits().getTotalHits();
        System.out.println("Hits: " + hits);
        assertEquals(1, hits);

    }

    private void waitUntilTypesAreCreatedInEs() {
        AdminClient client = esNode.client().admin();
        while (true) {
            try {
                IndicesExistsResponse response =
            //@formatter:off
            client.indices().exists(IndicesExistsAction.INSTANCE.newRequestBuilder(esNode.client())
                                                                   .setIndices(new String[] { "person-index" })
                                                                   .request())
                                                                   .actionGet();
            //@formatter:on
                if (response != null) {
                    if (response.isExists()) {
                        System.out.println("Index person-index exists now.");
                        waitFor(5000);
                        break;
                    }
                }
            } catch (ElasticsearchException e) {
                e.printStackTrace();
            }
            waitFor(1000);
        }
    }

    private void waitFor(long sleep) {
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {
        try {
            service.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        topology.shutdown();
    }

    private void produce() {
        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(producerProps);

        //@formatter:off
        Schema schema = SchemaBuilder.struct()
                                     .name("Person")
                                     .field("firstName", Schema.STRING_SCHEMA)
                                     .field("lastName", Schema.STRING_SCHEMA)
                                     .field("email", Schema.STRING_SCHEMA)
                                     .field("age", Schema.INT32_SCHEMA)
                                     .field("weightInKgs", Schema.INT32_SCHEMA)
                                     .build();

        Struct cartman = new Struct(schema)
                             .put("firstName", "Eric")
                             .put("lastName", "Cartman")
                             .put("email", "eric.cartman@southpark.com")
                             .put("age", 10)
                             .put("weightInKgs", 40);
        //@formatter:on

        JsonConverter converter = new JsonConverter();
        converter.configure(disableCache(Collections.emptyMap()), false);

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, null, converter.fromConnectData(topic, schema, cartman));
        producer.send(record);

        producer.flush();
        producer.close();
    }

    private static Map<String, Object> disableCache(Map<String, Object> original) {
        Map<String, Object> copy = new HashMap<>(original);
        copy.put("schemas.cache.size", 0);
        return copy;
    }

    private void consumeViaKafkaConnectToEs() {
        List<String> params = new ArrayList<String>();
        params.add(resource("connect-standalone.properties").getPath());
        params.add(resource("connect-es-sink.properties").getPath());

        try {
            ConnectStandalone.main(params.toArray(new String[params.size()]));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private URL resource(String name) {
        return Thread.currentThread().getContextClassLoader().getResource(name);
    }
}

package org.apache.kafka.connect.es;

import java.util.Collection;
import java.util.Map;
//import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig.ActionType;
import org.apache.kafka.connect.es.consumer.Consumer;
import org.apache.kafka.connect.es.consumer.DeleteConsumer;
import org.apache.kafka.connect.es.consumer.IndexConsumer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class ElasticSearchSinkTask extends SinkTask {

    private ElasticSearchSinkConnectorConfig config;
    private ElasticSearchClient client;
    private Consumer<SinkRecord> consumer;

    @Override
    public String version() {
        return Version.version();
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            config = new ElasticSearchSinkConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start " + ElasticSearchSinkConnector.class.getName() + " due to configuration error.", e);
        }

        client = new ElasticSearchClient(config);

        ActionType at = config.getActionType();
        switch (at) {
        case INDEX:
            consumer = new IndexConsumer(config, client.getBulkProcessor());
            break;
        case DELETE:
            consumer = new DeleteConsumer(config, client.getBulkProcessor());
            break;
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (consumer != null) {
            int count = 0;
            for (SinkRecord record : records) {
                boolean processed = consumer.process(record);
                if (processed) {
                    count++;
                }
            }
            client.updatePendingRequests(count);
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        client.waitUntilNonePending();
    }

    @Override
    public void stop() {
        client.stop();
    }
}

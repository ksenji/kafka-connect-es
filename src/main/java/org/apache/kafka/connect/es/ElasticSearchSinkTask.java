package org.apache.kafka.connect.es;

import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig.ActionType;
import org.apache.kafka.connect.es.consumer.DeleteConsumer;
import org.apache.kafka.connect.es.consumer.IndexConsumer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.elasticsearch.client.Client;

public class ElasticSearchSinkTask extends SinkTask {

	private ElasticSearchSinkConnectorConfig config;
	private Client client;
	private ActionType at;

	@Override
	public String version() {
		return Version.version();
	}

	@Override
	public void start(Map<String, String> props) {
		try {
			config = new ElasticSearchSinkConnectorConfig(props);
			client = config.getEsClient();
			at = config.getActionType();
		} catch (ConfigException e) {
			throw new ConnectException(
					"Couldn't start " + ElasticSearchSinkConnector.class.getName() + " due to configuration error.", e);
		}
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		Consumer<SinkRecord> consumer = null;
		switch (at) {
		case INDEX:
			consumer = new IndexConsumer(config);
			break;
		case DELETE:
			consumer = new DeleteConsumer(config);
			break;
		}

		if (consumer != null) {
			records.forEach(consumer);
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		config.flush();
	}

	@Override
	public void stop() {

	}
}

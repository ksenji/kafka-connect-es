package org.apache.kafka.connect.es.consumer;

import java.util.function.Consumer;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.elasticsearch.action.bulk.BulkProcessor;

public abstract class AbstractConsumer implements Consumer<SinkRecord> {

	protected ElasticSearchSinkConnectorConfig config;

	public AbstractConsumer(ElasticSearchSinkConnectorConfig config) {
		this.config = config;
	}

	@Override
	public void accept(SinkRecord record) {
		Converter converter = config.getValueConverter();
		byte[] data = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());

		addRequestToBulkProcessor(config.getEsBulkProcessor(), data);
	}

	protected abstract void addRequestToBulkProcessor(BulkProcessor processor, byte[] data);

}

package org.apache.kafka.connect.es.consumer;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Requests;

public class IndexConsumer extends AbstractConsumer {

	public IndexConsumer(ElasticSearchSinkConnectorConfig config) {
		super(config);
	}

	@Override
	protected void addRequestToBulkProcessor(BulkProcessor processor, byte[] data) {
		processor.add(Requests.indexRequest(config.getIndexName()).type(config.getTypeName()).source(data));
	}
}

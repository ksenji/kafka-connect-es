package org.apache.kafka.connect.es.consumer;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Requests;

public class IndexConsumer extends AbstractConsumer {

    public IndexConsumer(ElasticSearchSinkConnectorConfig config, BulkProcessor bulkProcessor) {
        super(config, bulkProcessor);
    }

    @Override
    protected boolean addRequestToBulkProcessor(BulkProcessor processor, byte[] data) {
        boolean success = false;
        if (data != null) {
            processor.add(Requests.indexRequest(config.getIndexName()).type(config.getTypeName()).source(data));
            success = true;
        }
        return success;
    }
}

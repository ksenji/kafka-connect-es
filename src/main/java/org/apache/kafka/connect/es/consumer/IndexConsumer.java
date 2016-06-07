package org.apache.kafka.connect.es.consumer;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class IndexConsumer extends AbstractConsumer {

    public IndexConsumer(ElasticSearchSinkConnectorConfig config, BulkProcessor bulkProcessor) {
        super(config, bulkProcessor);
    }

    @Override
    protected boolean addRequestToBulkProcessor(
        BulkProcessor processor, byte[] data, Optional<String> docIdGetter)
    {
        boolean success = false;
        if (data != null)
        {
            IndexRequest request = Requests.indexRequest(config.getIndexName())
                .type(config.getTypeName()) // fluent API returns self
                .source(data);

            if (docIdGetter.isPresent())
                request.id(docIdGetter.get());

            processor.add(request);
            success = true;
        }
        return success;
    }
}

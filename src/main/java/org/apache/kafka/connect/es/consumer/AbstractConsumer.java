package org.apache.kafka.connect.es.consumer;

//import java.util.function.Consumer;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.connect.es.converter.Converter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConsumer implements Consumer<SinkRecord> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected ElasticSearchSinkConnectorConfig config;
    protected BulkProcessor bulkProcessor;

    public AbstractConsumer(ElasticSearchSinkConnectorConfig config, BulkProcessor bulkProcessor) {
        this.config = config;
        this.bulkProcessor = bulkProcessor;
    }

    @Override
    public boolean process(SinkRecord record) {
        Converter converter = config.getConverter();
        byte[] data = converter.serialize(record);

        return addRequestToBulkProcessor(bulkProcessor, data);
    }

    protected abstract boolean addRequestToBulkProcessor(BulkProcessor processor, byte[] data);

}

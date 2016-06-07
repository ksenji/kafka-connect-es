package org.apache.kafka.connect.es.consumer;

//import java.util.function.Consumer;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.connect.es.converter.Converter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;

public abstract class AbstractConsumer implements Consumer<SinkRecord> {

    private Optional<Function<SinkRecord, String>> docIdGetter = Optional.empty();

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected ElasticSearchSinkConnectorConfig config;
    protected BulkProcessor bulkProcessor;


    public AbstractConsumer(ElasticSearchSinkConnectorConfig config, BulkProcessor bulkProcessor)
    {
        this.config = config;
        this.bulkProcessor = bulkProcessor;

        // build a type for the docIdGetter based on configuration
        this.docIdGetter = this.config.getDocIdSupplier();
    }

    @Override
    public boolean process(SinkRecord record)
    {
        Converter converter = config.getConverter();
        byte[] data = converter.serialize(record);

        Optional<String> docId = Optional.empty();
        if (docIdGetter.isPresent())
            docId = Optional.of(docIdGetter.get().apply(record));

        return addRequestToBulkProcessor(bulkProcessor, data, docId);
    }

    protected abstract boolean addRequestToBulkProcessor(
        BulkProcessor processor, byte[] data, Optional<String> docId);

}

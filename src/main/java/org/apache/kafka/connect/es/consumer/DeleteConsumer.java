package org.apache.kafka.connect.es.consumer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Requests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class DeleteConsumer extends AbstractConsumer {

    // private final Gson gson = new GsonBuilder().create();
    //
    // private static final Type TYPE = (new TypeToken<Map<String, Object>>() {
    // private static final long serialVersionUID = 1L;
    // }).getType();

    private ObjectMapper mapper = new ObjectMapper();
    private ObjectReader reader = mapper.reader();

    public DeleteConsumer(ElasticSearchSinkConnectorConfig config, BulkProcessor bulkProcessor) {
        super(config, bulkProcessor);
    }

    @Override
    protected boolean addRequestToBulkProcessor(BulkProcessor processor, byte[] data) {
        boolean successful = false;

        JsonNode tree = null;
        if (data != null) {
            try {
                tree = reader.readTree(new ByteArrayInputStream(data));
            } catch (IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("Error converting to JsonNode", e);
                }
            }
        }

        if (tree != null) {
            JsonNode id = tree.get("id");
            if (id != null) {
                processor.add(Requests.deleteRequest(config.getIndexName()).type(config.getTypeName()).id(id.asText()));
                successful = true;
            } else if (log.isDebugEnabled()) {
                log.debug("No id field found in the JSON: {}", tree);
            }
        }
        return successful;
    }
}

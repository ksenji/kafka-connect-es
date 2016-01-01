package org.apache.kafka.connect.es.consumer;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.Requests;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

public class DeleteConsumer extends AbstractConsumer {

	private final Gson gson = new GsonBuilder().create();

	private static final Type TYPE = (new TypeToken<Map<String, Object>>() {
		private static final long serialVersionUID = 1L;
	}).getType();

	public DeleteConsumer(ElasticSearchSinkConnectorConfig config) {
		super(config);
	}

	@Override
	protected void addRequestToBulkProcessor(BulkProcessor processor, byte[] data) {

		Map<String, Object> map = null;
		try {
			map = gson.fromJson(new String(data, "utf-8"), TYPE);
		} catch (JsonSyntaxException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}

		if (map != null) {
			Object id = map.get("id");
			if (id != null) {
				processor.add(Requests.deleteRequest(config.getIndexName()).type(config.getTypeName()).id(String.valueOf(id)));
			}
		}
	}
}

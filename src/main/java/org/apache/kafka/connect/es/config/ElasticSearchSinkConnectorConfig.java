package org.apache.kafka.connect.es.config;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.es.Dcl;
import org.apache.kafka.connect.storage.Converter;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Builder;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchSinkConnectorConfig extends AbstractConfig {

	private static final Logger log = LoggerFactory.getLogger(ElasticSearchSinkConnectorConfig.class);

	private static final String ES_CLUSTER = "es.cluster";
	private static final String ES_CLUSTER_DOC = "A comma separated Elastic search cluster including port with a :. For example localhost:9300";

	private static final String ES_CLUSTER_NAME = "es.cluster.name";
	private static final String ES_CLUSTER_NAME_DOC = "Elastic search cluster name";

	private static final String INDEX = "index";
	private static final String INDEX_DOC = "The name of elasticsearch index. Default is: kafka-index";

	private static final String TYPE = "type";
	private static final String TYPE_DOC = "The mapping type of elasticsearch index. Default is: status";

	private static final String BULK_SIZE = "bulk.size";
	private static final String BULK_SIZE_DOC = "The number of messages to be bulk indexed into elasticsearch. Default is: 100";

	private static final String ACTION_TYPE = "action.type";
	private static final String ACTION_TYPE_DOC = "The action type against how the messages should be processed. Default is: index. The following options are available: "
			+ "index : Creates documents in ES with the value field set to the received message. "
			+ "delete : Deletes documents from ES based on id field set in the received message. "
			+ "raw.execute : Execute incoming messages as a raw query.";

	private static final String ES_CONVERTER_CLASS_CONFIG = "es.converter";
	private static final String ES_CONVERTER_CLASS_DOC = "Converter class that converts Connect data to ES format.";

	static ConfigDef config = new ConfigDef()
			.define(ES_CLUSTER, Type.LIST, "localhost:9300", Importance.HIGH, ES_CLUSTER_DOC)
			.define(ES_CLUSTER_NAME, Type.STRING, "elasticsearch", Importance.HIGH, ES_CLUSTER_NAME_DOC)
			.define(INDEX, Type.STRING, "kafka-index", Importance.HIGH, INDEX_DOC)
			.define(TYPE, Type.STRING, "status", Importance.HIGH, TYPE_DOC)
			.define(BULK_SIZE, Type.INT, 100, Importance.HIGH, BULK_SIZE_DOC)
			.define(ACTION_TYPE, Type.STRING, "index", Importance.HIGH, ACTION_TYPE_DOC)
			.define(ES_CONVERTER_CLASS_CONFIG, Type.CLASS, Importance.MEDIUM, ES_CONVERTER_CLASS_DOC);

	private Map<String, String> originals;

	public ElasticSearchSinkConnectorConfig(Map<String, String> props) {
		super(config, props);
		this.originals = props;
	}

	private Dcl<Client> client = new Dcl<Client>() {
		@Override
		public Client init() {
			Settings settings = Settings.settingsBuilder().put("cluster.name", getString(ES_CLUSTER_NAME)).build();
			return TransportClient.builder().settings(settings).build()
					.addTransportAddresses(toTransportAddresses(getList(ES_CLUSTER)));
		}
	};

	private Dcl<BulkProcessor> processor = new Dcl<BulkProcessor>() {
		@Override
		public BulkProcessor init() {
			Builder builder = BulkProcessor.builder(client.get(), new Listener() {
				@Override
				public void beforeBulk(long executionId, BulkRequest request) {
					prevBulkRequest = request;
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
					prevBulkRequest = request;
					success.set(true);
				}

				@Override
				public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
					prevBulkRequest = request;
					success.set(false);
					List<ActionRequest> requests = request.requests();
					for (ActionRequest ar : requests) {
						getEsBulkProcessor().add(ar);
					}
				}
			});

			return builder.setConcurrentRequests(0).build();
		}
	};

	private AtomicBoolean success = new AtomicBoolean(false);
	private volatile BulkRequest prevBulkRequest = null;

	public void flush() {
		prevBulkRequest = null;
		BulkProcessor processor = getEsBulkProcessor();
		success.set(false);
		while (!success.get()) {
			processor.flush();
			boolean succeeded = success.get();
			if (!succeeded) {
				BulkRequest bulkRequest = prevBulkRequest;
				if (bulkRequest == null) {
					break;
				}
			}
		}
	}

	private TransportAddress[] toTransportAddresses(List<String> nodes) {
		List<TransportAddress> addresses = new ArrayList<>(nodes.size());
		for (String node : nodes) {
			String[] hostAndPort = node.split(":");

			String host = null;
			int port = 9300;

			if (hostAndPort.length >= 1) {
				host = hostAndPort[0];
			}

			if (hostAndPort.length >= 2) {
				try {
					port = Integer.parseInt(hostAndPort[1]);
				} catch (NumberFormatException e) {
					port = -1;
					if (log.isInfoEnabled()) {
						log.info("Ingoring invalid port: {}", port);
					}
				}
			}
			if (port != -1) {
				try {
					addresses.add(new InetSocketTransportAddress(InetAddress.getByName(host), port));
				} catch (UnknownHostException e) {
					if (log.isErrorEnabled()) {
						log.error("Unknow host, Ingoring this node", e);
					}
				}
			}
		}
		return addresses.toArray(new TransportAddress[addresses.size()]);
	}

	public enum ActionType {

		INDEX("index"), DELETE("delete");

		private String actionType;
		private static Map<String, ActionType> MAPPING = init();

		private ActionType(String actionType) {
			this.actionType = actionType;
		}

		public String toValue() {
			return actionType;
		}

		public static ActionType toType(String value) {
			return MAPPING.get(value);
		}

		public static Map<String, ActionType> init() {
			Map<String, ActionType> mapping = new HashMap<>();
			ActionType[] types = values();
			for (ActionType type : types) {
				mapping.put(type.name().toLowerCase(), type);
			}
			return mapping;
		}
	}

	public Client getEsClient() {
		return client.get();
	}

	public BulkProcessor getEsBulkProcessor() {
		return processor.get();
	}

	public ActionType getActionType() {
		return ActionType.toType(getString(ACTION_TYPE));
	}

	public String getIndexName() {
		return getString(INDEX);
	}

	public String getTypeName() {
		return getString(TYPE);
	}

	private Dcl<Converter> converter = new Dcl<Converter>() {
		@Override
		public Converter init() {
			Converter converter = null;
			try {
				Class<? extends Converter> klass = ElasticSearchSinkConnectorConfig.this
						.getClass(ES_CONVERTER_CLASS_CONFIG).asSubclass(Converter.class);
				if (klass != null) {
					converter = klass.newInstance();
					Map<String, String> props = new HashMap<>(originals);
					props.put("schemas.enable", Boolean.FALSE.toString());
					converter.configure(props, false);
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			return converter;
		}

	};

	public Converter getValueConverter() {
		return converter.get();
	}
}

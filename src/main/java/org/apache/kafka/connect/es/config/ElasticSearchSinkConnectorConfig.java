package org.apache.kafka.connect.es.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.es.converter.Converter;
import org.apache.kafka.connect.es.dcl.Factory;
import org.apache.kafka.connect.es.dcl.Factory.Dcl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchSinkConnectorConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchSinkConnectorConfig.class);

    public static final String ES_CLUSTER = "es.cluster";
    private static final String ES_CLUSTER_DOC = "A comma separated Elastic search cluster including port with a :. For example localhost:9300";

    public static final String ES_CLUSTER_NAME = "es.cluster.name";
    private static final String ES_CLUSTER_NAME_DOC = "Elastic search cluster name";

    public static final String INDEX = "index";
    private static final String INDEX_DOC = "The name of elasticsearch index. Default is: kafka-index";

    public static final String TYPE = "type";
    private static final String TYPE_DOC = "The mapping type of elasticsearch index. Default is: status";

    public static final String BULK_SIZE = "bulk.size";
    private static final String BULK_SIZE_DOC = "The number of messages to be bulk indexed into elasticsearch. Default is: 100";

    public static final String ACTION_TYPE = "action.type";
    private static final String ACTION_TYPE_DOC = "The action type against how the messages should be processed. Default is: index. The following options are available: "
            + "index : Creates documents in ES with the value field set to the received message. "
            + "delete : Deletes documents from ES based on id field set in the received message. ";

    public static final String ES_CONVERTER_CLASS_CONFIG = "es.converter";
    private static final String ES_CONVERTER_CLASS_DOC = "Converter class that converts Connect data to ES format.";

    static ConfigDef config = new ConfigDef().define(ES_CLUSTER, Type.LIST, "localhost:9300", Importance.HIGH, ES_CLUSTER_DOC)
            .define(ES_CLUSTER_NAME, Type.STRING, "elasticsearch", Importance.HIGH, ES_CLUSTER_NAME_DOC)
            .define(INDEX, Type.STRING, "kafka-index", Importance.HIGH, INDEX_DOC).define(TYPE, Type.STRING, "status", Importance.HIGH, TYPE_DOC)
            .define(BULK_SIZE, Type.INT, 100, Importance.HIGH, BULK_SIZE_DOC)
            .define(ACTION_TYPE, Type.STRING, "index", Importance.HIGH, ACTION_TYPE_DOC)
            .define(ES_CONVERTER_CLASS_CONFIG, Type.CLASS, Importance.MEDIUM, ES_CONVERTER_CLASS_DOC);

//    private Map<String, String> originals;

    public ElasticSearchSinkConnectorConfig(Map<String, String> props) {
        super(config, props);
//        this.originals = props;
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

    public ActionType getActionType() {
        return ActionType.toType(getString(ACTION_TYPE));
    }

    public String getIndexName() {
        return getString(INDEX);
    }

    public String getTypeName() {
        return getString(TYPE);
    }

    private Dcl<Converter> converter = Factory.of(() -> {
        Converter converter = null;
        try {
            Class<? extends Converter> klass = ElasticSearchSinkConnectorConfig.this.getClass(ES_CONVERTER_CLASS_CONFIG).asSubclass(Converter.class);
            if (klass != null) {
                converter = klass.newInstance();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return converter;

    });

    public Converter getConverter() {
        return converter.get();
    }
}

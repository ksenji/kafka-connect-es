package org.apache.kafka.connect.es;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.es.config.ElasticSearchSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;

public class ElasticSearchSinkConnector extends SinkConnector {

	private Map<String, String> configProperties;

	@Override
	public String version() {
		return Version.version();
	}

	@Override
	public void start(Map<String, String> props) {
		this.configProperties = props;
		try {
			new ElasticSearchSinkConnectorConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException("Couldn't start " + getClass().getName() + " due to configuration error.", e);
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return ElasticSearchSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>();
		taskProps.putAll(configProperties);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(taskProps);
		}
		return taskConfigs;
	}

	@Override
	public void stop() {
	}

}

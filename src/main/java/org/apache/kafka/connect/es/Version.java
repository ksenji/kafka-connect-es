package org.apache.kafka.connect.es;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.connect.es.dcl.Factory;
import org.apache.kafka.connect.es.dcl.Factory.Dcl;

public class Version {

	private static final Dcl<String> VERSION_INFO = Factory.of(() -> {
		Properties props = new Properties();
		try {
			props.load(Version.class.getResourceAsStream("/kafka-connect-es-version.properties"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return props.getProperty("version", "unknown").trim();
	});

	public static String version() {
		return VERSION_INFO.get();
	}
}

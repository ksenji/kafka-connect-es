package org.apache.kafka.connect.es.converter.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.es.converter.Converter;
import org.apache.kafka.connect.sink.SinkRecord;

public class KeyIgnoringJsonConverter implements Converter {
    
    private final org.apache.kafka.connect.json.JsonConverter valueConverter;

    public KeyIgnoringJsonConverter() {
        valueConverter = new org.apache.kafka.connect.json.JsonConverter();

        Map<String, String> props = new HashMap<>();
        props.put("schemas.enable", Boolean.FALSE.toString());
        valueConverter.configure(props, false);
    }

    @Override
    public byte[] serialize(SinkRecord record) {
        return valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
    }
}

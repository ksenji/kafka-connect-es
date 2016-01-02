package org.apache.kafka.connect.es.converter.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.es.converter.Converter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class KeyValueUnionJsonConverter implements Converter {

    private static final Logger log = LoggerFactory.getLogger(KeyValueUnionJsonConverter.class);

    private final org.apache.kafka.connect.json.JsonConverter valueConverter;
    private final org.apache.kafka.connect.json.JsonConverter keyConverter;

    private final ObjectMapper mapper;
    private final ObjectReader reader;

    public KeyValueUnionJsonConverter() {
        valueConverter = new org.apache.kafka.connect.json.JsonConverter();
        keyConverter = new org.apache.kafka.connect.json.JsonConverter();

        Map<String, String> props = new HashMap<>();
        props.put("schemas.enable", Boolean.FALSE.toString());
        valueConverter.configure(props, false);
        keyConverter.configure(props, true);

        mapper = new ObjectMapper();
        reader = mapper.reader();
    }

    @Override
    public byte[] serialize(SinkRecord record) {

        byte[] valueS = valueConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        byte[] keyS = keyConverter.fromConnectData(record.topic(), record.keySchema(), record.key());

        Map<String, Object> union = new HashMap<>();

        JsonNode keyJN = null;
        if (keyS != null) {
            try {
                keyJN = reader.readTree(new ByteArrayInputStream(keyS));
            } catch (IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("Error converting key to JsonNode", e);
                }
            }
        }

        JsonNode valueJN = null;
        if (valueS != null) {
            try {
                valueJN = reader.readTree(new ByteArrayInputStream(valueS));
            } catch (IOException e) {
                if (log.isErrorEnabled()) {
                    log.error("Error converting value to JsonNode", e);
                }
            }
        }

        Map keyM = (keyJN != null) ? mapper.convertValue(keyJN, Map.class) : null;
        Map valueM = (valueJN != null) ? mapper.convertValue(valueJN, Map.class) : null;

        if (keyM != null) {
            union.putAll(keyM);
        }
        if (valueM != null) {
            union.putAll(valueM);
        }

        byte[] unionB = null;
        try {
            unionB = mapper.writeValueAsBytes(union);
        } catch (JsonProcessingException e) {
            if (log.isErrorEnabled()) {
                log.error("Error writing union Map as bytes", e);
            }
        }
        return unionB;
    }
}

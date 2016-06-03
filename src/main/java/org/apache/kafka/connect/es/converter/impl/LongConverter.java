package org.apache.kafka.connect.es.converter.impl;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;
import java.util.HashMap;
/**
 * Created by rspurgeon on 5/27/2016.
 */
public class LongConverter implements Converter
{
    private final LongSerializer serializer = new LongSerializer();
    private final LongDeserializer deserializer = new LongDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
        Map<String, Object> serializerConfigs = new HashMap<>();
        serializerConfigs.putAll(configs);
        Map<String, Object> deserializerConfigs = new HashMap<>();
        deserializerConfigs.putAll(configs);

        serializer.configure(serializerConfigs, isKey);
        deserializer.configure(deserializerConfigs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value)
    {
        try {
            return serializer.serialize(topic, value == null ? null : ((Number) value).longValue());
        }
        catch (SerializationException se) {
            throw new DataException("failed to serialize to Long", se);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value)
    {
        try {
            return new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, deserializer.deserialize(topic, value));
        }
        catch (SerializationException se)
        {
            throw new DataException("failed to deserialize Long: ", se);
        }
    }
}

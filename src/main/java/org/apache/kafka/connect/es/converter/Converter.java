package org.apache.kafka.connect.es.converter;

import org.apache.kafka.connect.sink.SinkRecord;

public interface Converter {

    public byte[] /* JSON*/ serialize(SinkRecord record);
}

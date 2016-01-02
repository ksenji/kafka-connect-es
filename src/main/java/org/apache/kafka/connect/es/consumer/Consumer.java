package org.apache.kafka.connect.es.consumer;

@FunctionalInterface
public interface Consumer<SinkRecord> {

    public boolean process(SinkRecord record);
}

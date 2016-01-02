# kafka-connect-es

KafkaConnect (CopyCat) for writing data to ElasticSearch. The ElasticSearchSinkTask can be configured with the following configuration.

<pre>
<code>
es.cluster=127.0.0.1:9300<br/>
es.cluster.name=elasticsearch<br/>
index=person-index<br/>
type=person<br/>
bulk.size=1200<br/>
action.type=index<br/>
es.converter=org.apache.kafka.connect.es.converter.impl.KeyIgnoringJsonConverter
</code>
</pre>

`es.converter` is a org.apache.kafka.connect.es.converter.Converter that needs to be configured. This will take a SinkRecord object and serialize it in to JSON bytes that can be written to ElasticSearch.

If the data in Kafka is already in JSON format and if you ignore Key (or Key is null in Kafka) then you can use the `org.apache.kafka.connect.es.converter.impl.KeyIgnoringJsonConverter` that is available with this library.

There is `org.apache.kafka.connect.es.converter.impl.KeyValueUnionJsonConverter` Converter available which will combine both Key & Value and both need to be JSON data in Kafka.

If you have any other format in Kafka (for example Avro), you would have to code a Converter to convert a SinkRecord to JSON format.

This Sink takes care of fault tolerance. Only when all the records are successfully committed in ElasticSearch, it instructs KafkaConnect to procceed and commit offsets.
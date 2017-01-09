# KafkaIO : Dataflow Unbounded Source and Sink for Kafka Topics

KafkaIO provides unbounded sources and sinks for [Kafka](https://www.firebase.com/)
topics. Kafka version 0.9 and above are supported.

## Basic Usage

* Read from a topic with 8 byte long keys and string values:
```java
 PCollection<KV<Long, String>> kafkaRecords =
   pipeline
     .applY(KafkaIO.read()
       .withBootstrapServers("broker_1:9092,broker_2:9092")
       .withTopics(ImmutableList.of("topic_a"))
       .withKeyCoder(BigEndianLongCoder.of())
       .withValueCoder(StringUtf8Coder.of())
       .withoutMetadata()
     );
```

* Write the same PCollection to a Kafka topic:
```java
 kafkaRecords.apply(KafkaIO.write()
   .withBootstrapServers("broker_1:9092,broker_2:9092")
   .withTopic("results")
   .withKeyCoder(BigEndianLongCoder.of())
   .withValueCoder(StringUtf8Coder.of())
```

Please see JavaDoc for KafkaIO in
[KafkaIO.java](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/contrib/kafka/src/main/java/com/google/cloud/dataflow/contrib/kafka/KafkaIO.java#L100)
for complete documentation and a more descriptive usage example.

## Release Notes
  * **0.2.0** : Assign one split for each of the Kafka topic partitions. This makes Dataflow
                [Update](https://cloud.google.com/dataflow/pipelines/updating-a-pipeline)
                from previous version incompatible.
  * **0.1.0** : KafkaIO with support for Unbounded Source and Sink.

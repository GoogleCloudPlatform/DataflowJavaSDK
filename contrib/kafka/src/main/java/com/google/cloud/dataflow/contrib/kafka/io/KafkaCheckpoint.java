package com.google.cloud.dataflow.contrib.kafka.io;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.common.collect.Maps;

/**
 * A marker representing the progress and state of the {@link UnboundedReader}.
 *
 * Starts a separate thread to read messages from Kafka to populate a {@link
 * LinkedBlockingQueue}. As messages are read, the furthest processed offset is
 * tracked. When messages have been durably committed by Dataflow, the furthest
 * offset read is written to Kafka.
 *
 * Will return duplicate messages.
 */
public class KafkaCheckpoint implements UnboundedSource.CheckpointMark {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCheckpoint.class);

  private final LinkedBlockingQueue<ConsumerRecord<String, String>> recordQueue;
  private final ConcurrentMap<TopicPartition, OffsetAndMetadata> furthestCommitted;
  private Map<TopicPartition, OffsetAndMetadata> furthestRead;
  private ConsumerRecord<String, String> current;
  private KafkaProducer producer;

  KafkaCheckpoint(String bootstrapServers, String groupId, String topic) {
    recordQueue = new LinkedBlockingQueue<ConsumerRecord<String, String>>();
    furthestCommitted = Maps.newConcurrentMap();
    furthestRead = Maps.newHashMap();

    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    KafkaProducer producer = builder
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      .withTopic(topic)
      .withQueue(recordQueue)
      .withCommitted(furthestCommitted)
      .build();

    new Thread(producer).start();
  }

  /**
   * Commit the furthest read offsets to the Kafka cluster.
   */
  @Override
  public void finalizeCheckpoint() {
    furthestCommitted.clear();
    furthestCommitted.putAll(furthestRead);

    producer.commit();

    furthestRead = Maps.newHashMap();
  }

  /**
   * Advance to the next available record. Returns true if a record exists.
   * False otherwise.
   */
  public boolean advance() {
    current = this.recordQueue.poll();
    if (current == null) {
      return false;
    }

    TopicPartition p = new TopicPartition(current.topic(), current.partition());
    OffsetAndMetadata offset = new OffsetAndMetadata(current.offset() + 1);
    furthestRead.put(p, offset);

    return true;
  }

  /**
   * Return the current record.
   */
  public ConsumerRecord<String, String> current() throws NoSuchElementException {
    if (current == null) {
      throw new NoSuchElementException();
    }

    LOG.info("Current Record: topic={}, partition={}, offset={}, value={}",
        current.topic(), current.partition(), current.offset(), current.value());

    return current;
  }

  /**
   * Shutdown the producer thread.
   */
  public void shutdown() {
    producer.shutdown();
  }
}

package com.google.cloud.dataflow.contrib.kafka.io;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerWakeupException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Produce messages from Kafka for consumption by the KafkaReader.
 */
public class KafkaProducer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCheckpoint.class);
  private static final Integer DEFAULT_SESSION_TIMEOUT = 30000;

  private final LinkedBlockingQueue<ConsumerRecord<String, String>> queue;
  private final ConcurrentMap<TopicPartition, OffsetAndMetadata> committed;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final AtomicBoolean commit = new AtomicBoolean(false);
  private final KafkaConsumer<String, String> consumer;

  private KafkaProducer(
      String bootstrapServers,
      String groupId,
      String topic,
      Integer sessionTimeout,
      LinkedBlockingQueue<ConsumerRecord<String, String>> queue,
      ConcurrentMap<TopicPartition, OffsetAndMetadata> committed,
      KafkaConsumer<String, String> consumer) {

    if (bootstrapServers == null || bootstrapServers.isEmpty()) {
      throw new IllegalArgumentException("bootstrapServers can not be null.");
    }

    if (groupId == null || groupId.isEmpty()) {
      throw new IllegalArgumentException("groupId is required.");
    }

    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("topic is required.");
    }

    if (queue == null) {
      throw new IllegalArgumentException("queue is required.");
    }

    if (committed == null) {
      throw new IllegalArgumentException("committed is required.");
    }

    if (sessionTimeout == null) {
      sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    }

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", groupId);
    props.put("session.timeout.ms", Integer.toString(sessionTimeout));
    props.put("enable.auto.commit", "false");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    this.queue = queue;
    this.committed = committed;

    if (consumer == null) {
      this.consumer = new KafkaConsumer<String, String>(props);
    } else {
      this.consumer = consumer;
    }
    this.consumer.subscribe(Lists.newArrayList(topic));
  }

  public static class Builder
  {
    private String topic;
    private String groupId;
    private Integer sessionTimeout;
    private String bootstrapServers;
    private LinkedBlockingQueue<ConsumerRecord<String, String>> queue;
    private ConcurrentMap<TopicPartition, OffsetAndMetadata> committed;
    private KafkaConsumer<String, String> consumer;

    public Builder withTopic(final String topic) {
      this.topic = topic;
      return this;
    }

    public Builder withGroupId(final String groupId) {
      this.groupId = groupId;
      return this;
    }

    public Builder withSessionTimeout(final Integer sessionTimeout) {
      this.sessionTimeout = sessionTimeout;
      return this;
    }

    public Builder withBootstrapServers(final String bootstrapServers)
    {
       this.bootstrapServers = bootstrapServers;
       return this;
    }

    public Builder withQueue(final LinkedBlockingQueue<ConsumerRecord<String, String>> queue)
    {
      this.queue = queue;
      return this;
    }

    public Builder withCommitted(final ConcurrentMap<TopicPartition, OffsetAndMetadata> committed)
    {
      this.committed = committed;
      return this;
    }

    public Builder withConsumer(final KafkaConsumer<String, String> consumer)
    {
      this.consumer = consumer;
      return this;
    }

    public KafkaProducer build()
    {
      return new KafkaProducer(
          bootstrapServers,
          groupId,
          topic,
          sessionTimeout,
          queue,
          committed,
          consumer);
    }
  }

  /**
   * Poll for messages from Kafka and add to shared queue. Set commit to true to
   * commit all offsets specified in committed shared map.
   *
   */
  public void run() {
    try {
      while (!shutdown.get()) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
          try {
            queue.put(record);
          } catch (InterruptedException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
          }
        }

        // Sync all topics with their furthest read offset
        if (commit.get()) {
          System.out.println(committed);

          // Remove any committed partitions that we are no longer assigned to.
          // if we are no longer assigned, a consumer rebalance has shifted
          // those partitions to another consumer. Let the other consumer handle
          // those messages.
          Sets.SetView<TopicPartition> notAssigned =
              Sets.difference(committed.keySet(), consumer.assignment());
          committed.keySet().removeAll(notAssigned);

          for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : committed.entrySet()) {
            LOG.info("Committing: topic={}, partition={}, offset={}", entry.getKey().topic(),
                entry.getKey().partition(), entry.getValue().offset());
          }

          consumer.commitSync(committed);
          commit.set(false);
        }
      }
    } catch (ConsumerWakeupException e) {
      // Ignore exception if closing
      if (!shutdown.get()) throw e;
    } finally {
      consumer.close();
    }
  }

  /**
   * Commit the furthest read offset of each partition. If we are no longer
   * subscribed to a partition, the offset is not committed. Another consumer
   * will process these offsets. Duplicate data may result.
   */
  public synchronized void commit() {
    commit.set(true);
  }

  /**
   * Shutdown hook. Closes the consumer.
   */
  public synchronized void shutdown() {
    shutdown.set(true);
    consumer.wakeup();
  }
}

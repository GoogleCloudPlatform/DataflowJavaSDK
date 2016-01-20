
package com.workiva.cloud.dataflow.io;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;


import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link KafkaProducer}.
 */
@RunWith(JUnit4.class)
public class KafkaProducerTest {

  @Test(expected=IllegalArgumentException.class)
  public void builder_requiresBootstrapId() throws Exception {
    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    builder.withGroupId("test").withTopic("test").build();
  }

  @Test(expected=IllegalArgumentException.class)
  public void builder_requiresOneBootstrapId() throws Exception {
    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    builder.withBootstrapServers("").withGroupId("test").withTopic("test").build();
  }

  @Test(expected=IllegalArgumentException.class)
  public void builder_requiresGroupId() throws Exception {
    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    builder.withBootstrapServers("localhost:9092").withTopic("test").build();
  }

  @Test(expected=IllegalArgumentException.class)
  public void builder_requiresTopic() throws Exception {
    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    builder.withBootstrapServers("localhost:9092").withGroupId("test").build();
  }

  @Test(expected=IllegalArgumentException.class)
  public void builder_requiresQueue() throws Exception {
    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    builder.withBootstrapServers("localhost:9092")
      .withGroupId("test").withTopic("test").build();
  }

  @Test(expected=IllegalArgumentException.class)
  public void builder_requiresCommitted() throws Exception {
    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    builder.withBootstrapServers("localhost:9092")
      .withGroupId("test")
      .withTopic("test")
      .withQueue(Queues.newLinkedBlockingQueue())
      .build();
  }

  @Test
  public void builder_constructsProducer() throws Exception {
    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    KafkaProducer producer = builder
      .withBootstrapServers("localhost:9092")
      .withGroupId("test")
      .withTopic("test")
      .withQueue(Queues.newLinkedBlockingQueue())
      .withCommitted(Maps.newConcurrentMap())
      .build();

    assertNotNull(producer);
  }


  @Test
  public void producesRecordsToSharedQueue() throws Exception {
    Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = Maps.newHashMap();
    List<ConsumerRecord<String, String>> recordList = Lists.newArrayList();
    recordList.add(new ConsumerRecord<String, String>("topic", 10, 100, "key", "value"));
    recordMap.put(new TopicPartition("topic", 0), recordList);

    @SuppressWarnings("unchecked")
    KafkaConsumer<String, String> consumer = mock(KafkaConsumer.class);

    when(consumer.poll(1000))
        .thenReturn(new ConsumerRecords<String, String>(recordMap))
        .thenReturn(ConsumerRecords.<String, String>empty());

    when(consumer.assignment()).thenReturn(Sets.newHashSet(new TopicPartition("topic", 1)));

    LinkedBlockingQueue<ConsumerRecord<String, String>> queue = Queues.newLinkedBlockingQueue();
    ConcurrentMap<TopicPartition, OffsetAndMetadata> committed = Maps.newConcurrentMap();
    committed.put(new TopicPartition("topic", 0), new OffsetAndMetadata(9, ""));

    KafkaProducer.Builder builder = new KafkaProducer.Builder();
    KafkaProducer producer = builder
      .withBootstrapServers("localhost:9092")
      .withGroupId("test")
      .withTopic("test")
      .withQueue(queue)
      .withCommitted(committed)
      .withConsumer(consumer)
      .build();

    new Thread(producer).start();

    producer.commit();

    ConsumerRecord<String, String> record = queue.take();

    // Assigned a different partition, will commit nothing
    verify(consumer).commitSync(Maps.newConcurrentMap());

    assertEquals(record.topic(), "topic");
    assertEquals(record.partition(), 10);
    assertEquals(record.offset(), 100);
    assertEquals(record.key(), "key");
    assertEquals(record.value(), "value");

    producer.shutdown();

    verify(consumer).close();
  }
}

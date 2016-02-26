package com.google.cloud.dataflow.contrib.kafka;

import java.io.Serializable;

/**
 * TODO(rangadi): JavaDoc
 */
public class KafkaRecord<K, V> implements Serializable {

  private final String topic;
  private final int partition;
  private final long offset;
  private final K key;
  private final V value;

  public KafkaRecord(
      String topic,
      int partition,
      long offset,
      K key,
      V value) {

    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
    this.key = key;
    this.value = value;
  }

  public String getTopic() {
    return topic;
  }

  public int getPartition() {
    return partition;
  }

  public long getOffset() {
    return offset;
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return value;
  }
}

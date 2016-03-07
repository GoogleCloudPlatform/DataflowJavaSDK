/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.kafka;

import java.io.Serializable;

/**
 * KafkaRecord contains key and value of the record as well as metadata for the record (topic name,
 * partition id, and offset). This is essentially a serializable
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord}.
 */
public class KafkaRecord<K, V> implements Serializable {

  private final String topic;
  private final int partition;
  private final long offset;
  private final K key;
  private final V value; // XXX TODO: use KV<K, V> instead

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

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof KafkaRecord) {
      @SuppressWarnings("unchecked")
      KafkaRecord<Object, Object> other = (KafkaRecord<Object, Object>) obj;
      return topic.equals(other.topic)
          && partition == other.partition
          && offset == other.offset
          && key.equals(other.key) // XXX KV.equals()
          && value.equals(other.value);
    } else {
      return false;
    }
  }
}

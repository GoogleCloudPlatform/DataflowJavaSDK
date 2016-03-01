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

import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;

import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Checkpoint for an unbounded KafkaSource reader. Consists of Kafka topic name, partition id,
 * and the latest offset consumed so far.
 */
@DefaultCoder(SerializableCoder.class)
public class KafkaCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {

  private final List<PartitionMark> partitions;

  public KafkaCheckpointMark(List<PartitionMark> partitions) {
    this.partitions = partitions;
  }

  public List<PartitionMark> getPartitions() {
    return partitions;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    /*
     * nothing to do.
     *
     * we might want to support committing offset in Kafka, though it does not guarantee
     * no-duplicates, it could support Dataflow restart better. Unlike an update of a dataflow job,
     * a restart does not have checkpoint state. This secondary checkpoint might be a good start
     * for readers. Another similar benefit is when the number of workers or number of
     * Kafka partitions changes.
     */
  }

  /**
   * TopicPartition, offset tuple. Defines specific location in the partitions.
   */
  public static class PartitionMark implements Serializable {
    private final TopicPartition topicPartition;
    private final long offset;

    public PartitionMark(TopicPartition topicPartition, long offset) {
      this.topicPartition = topicPartition;
      this.offset = offset;
    }

    public TopicPartition getTopicPartition() {
      return topicPartition;
    }

    public long getOffset() {
      return offset;
    }
  }
}


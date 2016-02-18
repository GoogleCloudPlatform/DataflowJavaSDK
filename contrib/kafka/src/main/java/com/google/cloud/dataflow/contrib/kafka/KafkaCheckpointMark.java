package com.google.cloud.dataflow.contrib.kafka;

import java.io.IOException;
import java.util.List;

import org.apache.kafka.common.TopicPartition;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;

/**
 * Checkpoint for an unbounded KafkaSource reader. Consists of Kafka topic name, partition id,
 * and the latest offset consumed so far.
 *
 * @author rangadi
 */
@DefaultCoder(AvroCoder.class)
public class KafkaCheckpointMark implements UnboundedSource.CheckpointMark {

  private final List<PartitionMark> partitions;

  public KafkaCheckpointMark(List<PartitionMark> partitions) {
    this.partitions = partitions;
  }

  @SuppressWarnings("unused") // for AvroCoder
  private KafkaCheckpointMark() {
    partitions = null;
  }

  public List<PartitionMark> getPartitions() {
    return partitions;
  }

  @Override
  public void finalizeCheckpoint() throws IOException {
    /*
     * nothing to do.
     * we might want to support committing offset in Kafka, though it does not guarantee
     * no-duplicates, it could support Dataflow restart better.
     * Unlike an update of a dataflow job, a restart does not have checkpoint state.
     * This secondary checkpoint might be a good start for readers.
     * Another similar benefit is when the number of workers or number of Kafka partitions
     * changes.
     */
  }

  public static class PartitionMark {
    private final TopicPartition topicPartition;
    private final long offset;

    /**
     * TODO
     * @param topic
     * @param partition
     * @param offset
     * @param consumed
     */
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


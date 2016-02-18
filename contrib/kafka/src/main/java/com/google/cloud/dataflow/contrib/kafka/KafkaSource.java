/*
 * Copyright (C) 2015 The Google Inc.
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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.io.Closeables;

/**
 * TODO(rangadi)
 *
 * @author rangadi
 */
public class KafkaSource {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  /* TODO:
   *  - abstract out kafka interactions
   *  - should we let user specify key and value deserializers in kafka. I don't think so. key
   *    matters to Kafka only at the producer (hashCode is used for partition). the consumer does
   *    not care. We could use DataFlow coder or SerializableFunction.
   *  - leave a comment about optionally storing
   */

  private static class IdentityFn<T> implements SerializableFunction<T, T> {
    public T apply(T input) {
      return input;
    }
  }

  /**
   * A function that returns {@link Instant#now} as the timestamp for each generated element.
   */
  private static class NowTimestampFn<T extends Object> implements SerializableFunction<T, Instant> {
    @Override
    public Instant apply(T input) {
      return Instant.now();
    }
  }

  public static <K, V> Builder<K, V> unboundedSourceBuilder() {
    return new Builder<K, V>();
  }

  public static Builder<byte[], byte[]> unboundedByteSourceBuilder() {
    return new Builder<byte[], byte[]>()
      .withKeyDecoderFn(new IdentityFn<byte[]>())
      .withValueDecoderFn(new IdentityFn<byte[]>());
  }

  public static class Builder<K, V> {

    private List<String> topics;
    // future: let users specify subset of partitions to read
    private SerializableFunction<byte[], K> keyDecoderFn;
    private SerializableFunction<byte[], V> valueDecoderFn;
    private SerializableFunction<ConsumerRecord<K, V>, Instant> timestampFn =
        new NowTimestampFn<ConsumerRecord<K, V>>(); // default processing timestamp

    private ImmutableMap.Builder<String, Object> consumerConfigBuilder = ImmutableMap
        .<String, Object>builder()
        .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName())
        .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName())
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") // default to latest offset when last offset is unknown.
        .put("enable.auto.commit", false); // disable auto commit (may be enabled by the user)

    /**
     * set of properties that are not required or don't make sense
     */
    private static final Map<String, String> ignoredConsumerProperties = ImmutableMap.of(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Set keyDecoderFn instead",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Set valueDecoderFn instead"

        // "group.id", "enable.auto.commit", "auto.commit.interval.ms" :
        //     lets allow these, applications can have better resume point for restarts.
        );


    private Builder() {}

    /**
     * Set Kafka bootstrap servers (alternately, set "bootstrap.servers" Consumer property).
     */
    public Builder<K, V> withBootstrapServers(String bootstrapServers) {
      return withConsumerConfig("bootstrap.servers", bootstrapServers);
    }

    /**
     * Set Kafka topics to be consumed. This is required.
     */
    public Builder<K, V> withTopics(Collection<String> topics) {
      this.topics = ImmutableList.copyOf(topics);
      return this;
    }

    /**
     * Set a {@KafkaConsumer} configuration properties.
     * @see ConsumerConfig
     */
    public Builder<K, V> withConsumerConfig(String configKey, Object configValue) {
      Preconditions.checkArgument(ignoredConsumerProperties.containsKey(configKey),
          "No need to configure '%s'. %s", configKey, ignoredConsumerProperties.get(configKey));
      consumerConfigBuilder.put(configKey, configValue);
      return this;
    }

    public Builder<K, V> withKeyDecoderFn(
        SerializableFunction<byte[], K> keyDecoderFn) {
        this.keyDecoderFn = keyDecoderFn;
        return this;
    }

    public Builder<K, V> withValueDecoderFn(
        SerializableFunction<byte[], V> valueDecoderFn) {
        this.valueDecoderFn = valueDecoderFn;
        return this;
    }

    /**
     * Set a timestamp function. Default is the timestamp when the ConsumerRecord is processed
     * by {@UnboundedReader#advance()}
     */
    public Builder<K, V> withTimestampFn(
        SerializableFunction<ConsumerRecord<K, V>, Instant> timestampFn) {
      this.timestampFn = timestampFn;
      return this;
    }

    public UnboundedSource<ConsumerRecord<K, V>, KafkaCheckpointMark> build() {

      ImmutableMap<String, Object> consumerConfig = consumerConfigBuilder.build();

      Preconditions.checkNotNull(
          consumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
          "Kafka bootstrap servers should be set");
      Preconditions.checkNotNull(topics, "Kafka topics should be set");
      Preconditions.checkArgument(!topics.isEmpty(), "At least one topic is required");
      Preconditions.checkNotNull(keyDecoderFn, "Decoder for Kafka key bytes should be set");
      Preconditions.checkNotNull(valueDecoderFn, "Decoder for Kafka values bytes should be set");

      return new UnboundedKafkaSource<K, V>(
          consumerConfig,
          topics,
          keyDecoderFn,
          valueDecoderFn,
          timestampFn,
          ImmutableList.of() // no assigned partitions yet
          );
    }
  }

  /** Static class, prevent instantiation */
  private KafkaSource() {}

  private static class UnboundedKafkaSource<K, V>
      extends UnboundedSource<ConsumerRecord<K, V>, KafkaCheckpointMark> {

    private final ImmutableMap<String, Object> consumerConfig;
    private final List<String> topics;
    private final SerializableFunction<byte[], K> keyDecoderFn;
    private final SerializableFunction<byte[], V> valueDecoderFn;
    private final SerializableFunction<ConsumerRecord<K, V>, Instant> timestampFn;
    private final List<TopicPartition> assignedPartitions;

    public UnboundedKafkaSource(
        ImmutableMap<String, Object> consumerConfig,
        List<String> topics,
        SerializableFunction<byte[], K> keyDecoderFn,
        SerializableFunction<byte[], V> valueDecoderFn,
        SerializableFunction<ConsumerRecord<K, V>, Instant> timestampFn,
        List<TopicPartition> assignedPartitions) {

      this.consumerConfig = consumerConfig;
      this.topics = topics;
      this.keyDecoderFn = keyDecoderFn;
      this.valueDecoderFn = valueDecoderFn;
      this.timestampFn = timestampFn;
      this.assignedPartitions = ImmutableList.copyOf(assignedPartitions);
    }

    @Override
    public List<? extends UnboundedSource<ConsumerRecord<K, V>, KafkaCheckpointMark>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {

      // XXX : I was checking various Java 8 streams and collectors.. thats is the reason for heavy use them here :)

      KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(consumerConfig);

      List<TopicPartition> partitions;

      try {
        // fetch partitions for each topic and sort them in <paritionId, topic> order.
        // sort by partitionId so that topics are evenly distributed among the splits.

        partitions = topics
          .stream()
          .flatMap(topic -> consumer.partitionsFor(topic).stream())
          .map(partInfo -> new TopicPartition(partInfo.topic(), partInfo.partition()))
          .sorted((p1, p2) -> ComparisonChain.start() // sort by <partition, topic>
              .compare(p1.partition(), p2.partition())
              .compare(p1.topic(), p2.topic())
              .result())
          .collect(Collectors.toList());
      } finally {
        consumer.close();
      }

      Preconditions.checkArgument(desiredNumSplits > 0);
      Preconditions.checkState(partitions.size() > 0,
          "Could not find any partitions. Please check Kafka configuration and topic names");

      int numSplits = Math.min(desiredNumSplits, partitions.size());

      Map<Integer, List<Integer>> assignments = IntStream.range(0, partitions.size())
          .mapToObj(i -> i)
          .collect(Collectors.groupingBy(i -> i % numSplits)); // groupingBy preserves order.

      // create a new source for each split with the assigned partitions for the split
      return IntStream.range(0, numSplits)
          .mapToObj(split -> {

            List<TopicPartition> assignedToSplit = assignments.get(split)
                .stream()
                .map(i -> partitions.get(i))
                .collect(Collectors.toList());

            LOG.info("Partitions assigned for split %d : %s",
                split, Joiner.on(",").join(assignedToSplit));

            // copy of 'this', except for assignedPartitions, which is replaced by assignedToSplit
            return new UnboundedKafkaSource<K, V>(
                this.consumerConfig,
                this.topics,
                this.keyDecoderFn,
                this.valueDecoderFn,
                this.timestampFn,
                assignedToSplit);
          })
          .collect(Collectors.toList());
    }

    @Override
    public UnboundedReader<ConsumerRecord<K, V>> createReader(
        PipelineOptions options,
        KafkaCheckpointMark checkpointMark) {
      return new UnboundedKafkaReader<K, V>(this, checkpointMark);
    }

    @Override
    public Coder<KafkaCheckpointMark> getCheckpointMarkCoder() {
      return AvroCoder.of(KafkaCheckpointMark.class);
    }

    @Override
    public boolean requiresDeduping() {
      return false;
    }

    @Override
    public void validate() {
      // TODO anything to do here?
    }

    @Override
    public Coder<ConsumerRecord<K, V>> getDefaultOutputCoder() {
      // no coder required. user explicitly provides functions to decode key and value
      return null;
    }
  }

  private static class UnboundedKafkaReader<K, V>
             extends UnboundedReader<ConsumerRecord<K, V>> {

    private final UnboundedKafkaSource<K, V> source;
    private KafkaConsumer<byte[], byte[]> consumer;

    private final boolean isRawSource; // i.e. if key and value decoders are identity functions
    private final long[] consumedOffsets; // consumed offset of each of the partitions,
                                          // initialized to offset from checkpoint or -1.

    // <topic, partitionId> --> index into consumerOffsets for this TopicPartition.
    private final Map<String, Map<Integer, Integer>> offsetIndexMap;

    private ConsumerRecord<K, V> curRecord;
    private Instant curTimestamp;

    private Iterator<ConsumerRecord<byte[], byte[]>> curBatch = Iterators.emptyIterator();

    public UnboundedKafkaReader(
        UnboundedKafkaSource<K, V> source,
        @Nullable KafkaCheckpointMark checkpointMark) {
      this.source = source;
      this.isRawSource = source.keyDecoderFn instanceof IdentityFn
          && source.valueDecoderFn instanceof IdentityFn;

      consumedOffsets = new long[source.assignedPartitions.size()];

      // a) verify that assigned and check-pointed partitions match
      // b) set consumed offsets
      if (checkpointMark != null) {
        Preconditions.checkState(
            checkpointMark.getPartitions().size() == source.assignedPartitions.size(),
            "checkPointMark and assignedPartitions should match");
        // we could consider allowing a mismatch, though it is not expected in current Dataflow

        for (int i=0; i < consumedOffsets.length; i++) {
          KafkaCheckpointMark.PartitionMark ckptMark = checkpointMark.getPartitions().get(i);
          TopicPartition assigned = source.assignedPartitions.get(i);

          Preconditions.checkState(ckptMark.getTopicPartition().equals(assigned),
              "checkpointed partition %s and assinged partition %s don't match at position %d",
              ckptMark.getTopicPartition(), assigned, i);


          consumedOffsets[i] = checkpointMark.getPartitions().get(i).getOffset();
        }
      } else {
        Arrays.fill(consumedOffsets, -1L);
      }

      //make ImmutableMap(topic -> ImmutableMap(paritionId -> index in offsets array))
      offsetIndexMap = ImmutableMap.copyOf(IntStream
          .range(0, source.assignedPartitions.size())
          .mapToObj(idx -> KV.of(source.assignedPartitions.get(idx), idx))
          .collect(Collectors.groupingBy(e -> e.getKey().topic(),
                   Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue())))
          .entrySet() // Entry<topic -> Map<partition -> index>>
          .stream()
          .collect(Collectors.toMap(e -> e.getKey(), e -> ImmutableMap.copyOf(e.getValue()))));
    }

    @Override
    public boolean start() throws IOException {

      consumer = new KafkaConsumer<>(source.consumerConfig);
      consumer.assign(source.assignedPartitions);

      // seek to offset if resuming
      for(int i=0; i<consumedOffsets.length; i++) {
        long offset = consumedOffsets[i];
        TopicPartition partition = source.assignedPartitions.get(i);

        if (offset >= 0) {
          LOG.info("Reader: resuming %s at %d", partition, offset+1);
          consumer.seek(partition, offset+1);
        } else {
          LOG.info("Reader: resuming from default offset for " + partition);
        }
      }

      curBatch = consumer.poll(10).iterator();

      return curBatch.hasNext();
    }

    @SuppressWarnings("unchecked") // for rawRecord to curRecord cast below
    @Override
    public boolean advance() throws IOException {
      while (true) {
        if (curBatch.hasNext()) {
          ConsumerRecord<byte[], byte[]> rawRecord = curBatch.next();

          int idx = offsetIndexMap.get(rawRecord.topic()).get(rawRecord.partition());
          long consumedOffset = consumedOffsets[idx];

          if (consumedOffsets[idx] >= 0 && rawRecord.offset() <= consumedOffset) {
            // this can happen when compression is enabled in kafka
            // should we check if the offset is way off from consumedOffset (say 1M more or less)

            LOG.info("ignoring already consumed offset %d for %s", rawRecord.offset(),
                source.assignedPartitions.get(idx));
            // TODO: increment a counter?

            continue;
          } else {

            // apply user decoders
            if (isRawSource) {
              // is shortcut this worth it? mostly not.
              curRecord = (ConsumerRecord<K, V>) rawRecord;
            } else {
              curRecord = new ConsumerRecord<K, V>(
                  rawRecord.topic(),
                  rawRecord.partition(),
                  rawRecord.offset(),
                  source.keyDecoderFn.apply(rawRecord.key()),
                  source.valueDecoderFn.apply(rawRecord.value()));
            }

            curTimestamp = source.timestampFn.apply(curRecord);
            consumedOffsets[idx] = rawRecord.offset();

            return true;
          }
        } else {
          // try to read next batch
          curBatch = consumer.poll(10).iterator(); // what should the timeout be 0 or something large?
          if (!curBatch.hasNext())
            return false;
        }
      }
    }

    @Override
    public Instant getWatermark() {
      return source.timestampFn.apply(curRecord);
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new KafkaCheckpointMark(IntStream
          .range(0, consumedOffsets.length)
          .mapToObj(i -> new KafkaCheckpointMark.PartitionMark(
              source.assignedPartitions.get(i), consumedOffsets[i]))
          .collect(Collectors.toList()));
    }

    @Override
    public UnboundedSource<ConsumerRecord<K, V>, ?> getCurrentSource() {
      return null;
    }

    @Override
    public ConsumerRecord<K, V> getCurrent() throws NoSuchElementException {
      // TODO: should we delay updating consumed offset till now?
      return curRecord;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return curTimestamp; //TODO: how is this related to getWatermark();
    }

    @Override
    public void close() throws IOException {
      Closeables.closeQuietly(consumer);
    }
  }
}

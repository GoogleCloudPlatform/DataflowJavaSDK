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
import java.io.Serializable;
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
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.io.Closeables;

/**
 * TODO(rangadi)
 *
 * @author rangadi
 */
public class KafkaSource {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  /* TODO: Overall todos:
   *    - javadoc at many places
   *    - confirm non-blocking behavior in advance()
   */

  private static SerializableFunction<byte[], byte[]> identityFn = bytes -> bytes;

  public static <K, V> Builder<K, V> unboundedSourceBuilder() {
    return new Builder<K, V>();
  }

  public static Builder<byte[], byte[]> unboundedByteSourceBuilder() {
    return new Builder<byte[], byte[]>()
      .withKeyDecoderFn(identityFn)
      .withValueDecoderFn(identityFn);
  }

  /**
   * Similar to {@link #unboundedSourceBuilder()}, except the the source strips KafkaRecord wrapper
   * and returns just the value.
   */
  public static <T extends Serializable> ValueSourceBuilder<T> unboundedValueSourceBuilder() {
    return new ValueSourceBuilder<T>(
       new Builder<byte[], T>()
       .withKeyDecoderFn(identityFn));
  }

  public static class Builder<K, V> {

    private List<String> topics;
    // future: let users specify subset of partitions to read
    private SerializableFunction<byte[], K> keyDecoderFn;
    private SerializableFunction<byte[], V> valueDecoderFn;
    private SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn = input -> Instant.now();

    private Map<String, Object> mutableConsumerConfig = Maps.newHashMap();

    /**
     * set of properties that are not required or don't make sense
     */
    private static final Map<String, String> ignoredConsumerProperties = ImmutableMap.of(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Set keyDecoderFn instead",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Set valueDecoderFn instead"

        // "group.id", "enable.auto.commit", "auto.commit.interval.ms" :
        //     lets allow these, applications can have better resume point for restarts.
        );


    private Builder() {
      // set config defaults
      mutableConsumerConfig.putAll(ImmutableMap.of(
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest", // default to latest offset when last offset is unknown.
          "enable.auto.commit", false)); // disable auto commit (may be enabled by the user)
    }

    /**
     * Set Kafka bootstrap servers (alternately, set "bootstrap.servers" Consumer property).
     */
    public Builder<K, V> withBootstrapServers(String bootstrapServers) {
      return withConsumerProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
    public Builder<K, V> withConsumerProperty(String configKey, Object configValue) {
      Preconditions.checkArgument(!ignoredConsumerProperties.containsKey(configKey),
          "No need to configure '%s'. %s", configKey, ignoredConsumerProperties.get(configKey));
      mutableConsumerConfig.put(configKey, configValue);
      return this;
    }

    /**
     * Update consumer config properties. Note that this does not not discard already configurured.
     * Same as invoking #withConsumerProperty() with each entry.
     */
    public Builder<K, V> withConsumerProperties(Map<String, Object> configToUpdate) {
      configToUpdate.entrySet().stream().forEach(
        e -> withConsumerProperty(e.getKey(), e.getValue()));
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
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      this.timestampFn = timestampFn;
      return this;
    }

    public UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> build() {

      ImmutableMap<String, Object> consumerConfig = ImmutableMap.copyOf(mutableConsumerConfig);

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
      extends UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> {

    private final ImmutableMap<String, Object> consumerConfig;
    private final List<String> topics;
    private final SerializableFunction<byte[], K> keyDecoderFn;
    private final SerializableFunction<byte[], V> valueDecoderFn;
    private final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn;
    private final List<TopicPartition> assignedPartitions;

    public UnboundedKafkaSource(
        ImmutableMap<String, Object> consumerConfig,
        List<String> topics,
        SerializableFunction<byte[], K> keyDecoderFn,
        SerializableFunction<byte[], V> valueDecoderFn,
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn,
        List<TopicPartition> assignedPartitions) {

      this.consumerConfig = consumerConfig;
      this.topics = topics;
      this.keyDecoderFn = keyDecoderFn;
      this.valueDecoderFn = valueDecoderFn;
      this.timestampFn = timestampFn;
      this.assignedPartitions = ImmutableList.copyOf(assignedPartitions);
    }

    @Override
    public List<UnboundedKafkaSource<K, V>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {

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

            LOG.info("Partitions assigned for split {} : {}",
                split, Joiner.on(",").join(assignedToSplit));

            // copy of 'this' with assignedPartitions replaced with assignedToSplit
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
    public UnboundedKafkaReader<K, V> createReader(PipelineOptions options,
                                                   KafkaCheckpointMark checkpointMark) {
      return new UnboundedKafkaReader<K, V>(this, checkpointMark);
    }

    @Override
    public Coder<KafkaCheckpointMark> getCheckpointMarkCoder() {
      return SerializableCoder.of(KafkaCheckpointMark.class);
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
    public Coder<KafkaRecord<K, V>> getDefaultOutputCoder() {
      // no coder is logically needed. user explicitly provides functions to decode key and value
      // sdk requires a Coder.
      return SerializableCoder.of(new TypeDescriptor<KafkaRecord<K,V>>() {});
    }
  }

  private static class UnboundedKafkaReader<K, V>
             extends UnboundedReader<KafkaRecord<K, V>> {

    private final UnboundedKafkaSource<K, V> source;
    private KafkaConsumer<byte[], byte[]> consumer;

    // maintains state of each assigned partition
    private static class PartitionState implements Iterator<PartitionState> {
      private final TopicPartition topicPartition;

      private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Iterators.emptyIterator();
      private ConsumerRecord<byte[], byte[]> record = null;

      private long consumedOffset;

      PartitionState(TopicPartition partition, long offset) {
        this.topicPartition = partition;
        this.consumedOffset = offset;
      }

      @Override
      public boolean hasNext() {
        return recordIter.hasNext();
      }

      @Override
      public PartitionState next() {
        record = recordIter.next();
        return this;
      }
    }

    private List<PartitionState> partitionStates;

    private KafkaRecord<K, V> curRecord;
    private Instant curTimestamp;

    private Iterator<PartitionState> curBatch = Iterators.emptyIterator();

    public UnboundedKafkaReader(
        UnboundedKafkaSource<K, V> source,
        @Nullable KafkaCheckpointMark checkpointMark) {

      this.source = source;

      partitionStates = ImmutableList.copyOf(source.assignedPartitions
          .stream()
          .map(tp -> new PartitionState(tp, -1L))
          .iterator());

      // a) verify that assigned and check-pointed partitions match
      // b) set consumed offsets

      if (checkpointMark != null) {
        Preconditions.checkState(
            checkpointMark.getPartitions().size() == source.assignedPartitions.size(),
            "checkPointMark and assignedPartitions should match");
        // we could consider allowing a mismatch, though it is not expected in current Dataflow

        for (int i=0; i < source.assignedPartitions.size(); i++) {
          KafkaCheckpointMark.PartitionMark ckptMark = checkpointMark.getPartitions().get(i);
          TopicPartition assigned = source.assignedPartitions.get(i);

          Preconditions.checkState(ckptMark.getTopicPartition().equals(assigned),
              "checkpointed partition %s and assinged partition %s don't match at position %d",
              ckptMark.getTopicPartition(), assigned, i);

          partitionStates.get(i).consumedOffset = ckptMark.getOffset();
        }
      }
    }

    private void readNextBatch() {
      // read one batch of records. one consumer.poll()

      ConsumerRecords<byte[], byte[]> records = consumer.poll(10); // what should the timeout be?

      // increment a counter or stat?

      partitionStates.stream().forEach(p -> {
        p.recordIter = records.records(p.topicPartition).iterator();
        p.record = null;
      });

      curBatch = Iterators.concat(partitionStates.iterator());
    }

    @Override
    public boolean start() throws IOException {

      consumer = new KafkaConsumer<>(source.consumerConfig);
      consumer.assign(source.assignedPartitions);

      // seek to next offset if consumedOffset is set
      partitionStates.stream().forEach(p -> {
        if (p.consumedOffset >= 0) {
          LOG.info("Reader: resuming {} at {}", p.topicPartition, p.consumedOffset + 1);
          consumer.seek(p.topicPartition, p.consumedOffset + 1);
        } else {
          LOG.info("Reader: resuming from default offset for {}", p.topicPartition);
        }
      });

      readNextBatch();

      return curBatch.hasNext();
    }

    @Override
    public boolean advance() throws IOException {
      while (true) {
        if (curBatch.hasNext()) {
          PartitionState pState = curBatch.next();

          ConsumerRecord<byte[], byte[]> rawRecord = pState.record;
          long consumed = pState.consumedOffset;
          long offset = rawRecord.offset();

          if (consumed >= 0 && offset <= consumed) {
            // this can happen when compression is enabled in Kafka
            // should we check if the offset is way off from consumedOffset (say 1M more or less)
            LOG.info("ignoring already consumed offset {} for {}",
                rawRecord.offset(), pState.topicPartition);

            // TODO: increment a counter?
            continue;

          } else {
            // sanity check
            if (consumed >= 0 && (offset - consumed) != 1) {
              LOG.warn("gap in offsets for {} after {}. {} records missing.",
                  pState.topicPartition, consumed, offset - consumed - 1);
            }

            // apply user decoders
            curRecord = new KafkaRecord<K, V>(
                rawRecord.topic(),
                rawRecord.partition(),
                rawRecord.offset(),
                source.keyDecoderFn.apply(rawRecord.key()),
                source.valueDecoderFn.apply(rawRecord.value()));

            curTimestamp = source.timestampFn.apply(curRecord);
            pState.consumedOffset = rawRecord.offset();

            return true;
          }
        } else {
          readNextBatch();

          if (!curBatch.hasNext())
            return false;
        }
      }
    }

    @Override
    public Instant getWatermark() {
      //XXX what should do? why is curRecord is null? return source.timestampFn.apply(curRecord);
      LOG.warn("curRec is {}. curTimestamp : {}, numPartitions {} : maxOffset : {}",
          (curRecord == null) ? "null" : "not null", curTimestamp, partitionStates.size(),
          partitionStates.stream().collect(Collectors.summarizingLong(s -> s.consumedOffset)).getMax());

      Instant timestamp = curRecord == null ? Instant.now() : curTimestamp;
      return timestamp.minus(Duration.standardMinutes(2));
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new KafkaCheckpointMark(partitionStates
          .stream()
          .map(p -> new KafkaCheckpointMark.PartitionMark(p.topicPartition, p.consumedOffset))
          .collect(Collectors.toList()));
    }

    @Override
    public UnboundedSource<KafkaRecord<K, V>, ?> getCurrentSource() {
      return source;
    }

    @Override
    public KafkaRecord<K, V> getCurrent() throws NoSuchElementException {
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

  // Builder, Source, Reader wrappers when user is only interested in Value in KafkaRecord :

  public static class ValueSourceBuilder<T extends Serializable> {
    // TODO : remove 'extends Serializable' restriction or improve it to just require a Coder<T>

    private Builder<byte[], T> underlying;

    private ValueSourceBuilder(Builder<byte[], T> underlying) {
      this.underlying = underlying;
    }

    public ValueSourceBuilder<T> withBootstrapServers(String bootstrapServers) {
      return new ValueSourceBuilder<T>(underlying.withBootstrapServers(bootstrapServers));
    }

    public ValueSourceBuilder<T> withTopics(Collection<String> topics) {
      return new ValueSourceBuilder<T>(underlying.withTopics(topics));
    }

    public ValueSourceBuilder<T> withConsumerProperty(String configKey, Object configValue) {
      return new ValueSourceBuilder<T>(underlying.withConsumerProperty(configKey, configValue));
    }

    public ValueSourceBuilder<T> withConsumerProperties(Map<String, Object> configToUpdate) {
      return new ValueSourceBuilder<T>(underlying.withConsumerProperties(configToUpdate));
    }

    public ValueSourceBuilder<T> withValueDecoderFn(SerializableFunction<byte[], T> valueDecoderFn) {
      return new ValueSourceBuilder<T>(underlying.withValueDecoderFn(valueDecoderFn));
    }

    public ValueSourceBuilder<T> withTimestampFn(SerializableFunction<T, Instant> timestampFn) {
      return new ValueSourceBuilder<T>(
          underlying.withTimestampFn(record -> timestampFn.apply(record.getValue())));
    }

    public UnboundedSource<T, KafkaCheckpointMark> build() {
      return new UnboundedKafkaValueSource<T>((UnboundedKafkaSource<byte[], T>) underlying.build());
    }
  }

  /**
   * Usually the users are only interested in value in KafkaRecord. This is a convenient class
   * to strip out other fields in KafkaRecord returned by UnboundedKafkaValueSource
   */
  private static class UnboundedKafkaValueSource<T extends Serializable> extends UnboundedSource<T, KafkaCheckpointMark> {

    private final UnboundedKafkaSource<?, T> underlying;

    public UnboundedKafkaValueSource(UnboundedKafkaSource<?, T> underlying) {
      this.underlying = underlying;
    }

    @Override
    public List<UnboundedKafkaValueSource<T>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return underlying
          .generateInitialSplits(desiredNumSplits, options)
          .stream()
          .map(s -> new UnboundedKafkaValueSource<T>(s))
          .collect(Collectors.toList());
    }

    @Override
    public UnboundedReader<T> createReader(PipelineOptions options, KafkaCheckpointMark checkpointMark) {
      return new UnboundedKafkaValueReader<T>(this, underlying.createReader(options, checkpointMark));
    }

    @Override
    public Coder<KafkaCheckpointMark> getCheckpointMarkCoder() {
      return underlying.getCheckpointMarkCoder();
    }

    @Override
    public void validate() {
      underlying.validate();
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return SerializableCoder.of(new TypeDescriptor<T>() {});
    }
  }

  private static class UnboundedKafkaValueReader<T extends Serializable> extends UnboundedReader<T> {

    private final UnboundedKafkaValueSource<T> source;
    private final UnboundedKafkaReader<?, T> underlying;

    public UnboundedKafkaValueReader(UnboundedKafkaValueSource<T> source,
                                     UnboundedKafkaReader<?, T> underlying) {
      this.source = source;
      this.underlying = underlying;
    }

    @Override
    public boolean start() throws IOException {
      return underlying.start();
    }

    @Override
    public boolean advance() throws IOException {
      return underlying.advance();
    }

    @Override
    public Instant getWatermark() {
      return underlying.getWatermark();
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return underlying.getCheckpointMark();
    }

    @Override
    public UnboundedKafkaValueSource<T> getCurrentSource() {
      return source;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return underlying.getCurrent().getValue();
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return underlying.getCurrentTimestamp();
    }

    @Override
    public void close() throws IOException {
      underlying.close();
    }
  }

}

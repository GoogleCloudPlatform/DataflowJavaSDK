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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.util.Maps;
import com.google.cloud.dataflow.contrib.kafka.KafkaCheckpointMark.PartitionMark;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.ExposedByteArrayInputStream;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

/**
 * TODO(rangadi): JavaDoc
 */
public class KafkaSource {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  /* TODO: Overall todos:
   *    - javadoc at many places
   *    - confirm non-blocking behavior in advance()
   */

  private static class NowTimestampFn<T> implements SerializableFunction<T, Instant> {
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
      .withKeyCoder(ByteArrayCoder.of())
      .withValueCoder(ByteArrayCoder.of());
  }

  /**
   * Similar to {@link #unboundedSourceBuilder()}, except the the source strips KafkaRecord wrapper
   * and returns just the value.
   */
  public static <T extends Serializable> ValueSourceBuilder<T> unboundedValueSourceBuilder() {
    return new ValueSourceBuilder<T>(
       new Builder<byte[], T>()
       .withKeyCoder(ByteArrayCoder.of()));
  }

  public static class Builder<K, V> {

    private List<String> topics;
    // future: let users specify subset of partitions to read
    private Coder<K> keyCoder;
    private Coder<V> valueCoder;
    private SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn = new NowTimestampFn<>();
    private Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn = Optional.absent();

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
     * Update consumer config properties. Note that this does not not discard already configured.
     * Same as invoking #withConsumerProperty() with each entry.
     */
    public Builder<K, V> withConsumerProperties(Map<String, Object> configToUpdate) {
      for(Entry<String, Object> e : configToUpdate.entrySet()) {
        withConsumerProperty(e.getKey(), e.getValue());
      }
      return this;
    }

    public Builder<K, V> withKeyCoder(Coder<K> keyCoder) {
        this.keyCoder = keyCoder;
        return this;
    }

    public Builder<K, V> withValueCoder(Coder<V> valueCoder) {
        this.valueCoder = valueCoder;
        return this;
    }

    /**
     * A function to assign a timestamp to a record. Default is the timestamp when the
     * record is processed by {@UnboundedReader#advance()}
     */
    public Builder<K, V> withTimestampFn(
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      this.timestampFn = timestampFn;
      return this;
    }

    /**
     * A function to calculate watermark. When this is not set, last record timestamp is returned
     * in {@link UnboundedReader#getWatermark()}.
     */
    public Builder<K, V> withWatermarkFn(SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {
      this.watermarkFn = Optional.of(watermarkFn);
      return this;
    }

    public UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> build() {

      ImmutableMap<String, Object> consumerConfig = ImmutableMap.copyOf(mutableConsumerConfig);

      Preconditions.checkNotNull(
          consumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
          "Kafka bootstrap servers should be set");
      Preconditions.checkNotNull(topics, "Kafka topics should be set");
      Preconditions.checkArgument(!topics.isEmpty(), "At least one topic is required");
      Preconditions.checkNotNull(keyCoder, "Coder for Kafka key bytes is required");
      Preconditions.checkNotNull(valueCoder, "Coder for Kafka values bytes is required");

      return new UnboundedKafkaSource<K, V>(
          consumerConfig,
          topics,
          keyCoder,
          valueCoder,
          timestampFn,
          watermarkFn,
          ImmutableList.<TopicPartition>of() // no assigned partitions yet
          );
    }
  }

  /** Static class, prevent instantiation */
  private KafkaSource() {}

  private static class UnboundedKafkaSource<K, V>
      extends UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> {

    private final ImmutableMap<String, Object> consumerConfig;
    private final List<String> topics;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn;
    // would it be a good idea to pass currentTimestamp to watermarkFn?
    private final Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn;
    private final List<TopicPartition> assignedPartitions;

    public UnboundedKafkaSource(
        ImmutableMap<String, Object> consumerConfig,
        List<String> topics,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn,
        Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn,
        List<TopicPartition> assignedPartitions) {

      this.consumerConfig = consumerConfig;
      this.topics = topics;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.timestampFn = timestampFn;
      this.watermarkFn = watermarkFn;
      this.assignedPartitions = ImmutableList.copyOf(assignedPartitions);
    }

    @Override
    public List<UnboundedKafkaSource<K, V>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {

      KafkaConsumer<K, V> consumer = new KafkaConsumer<K, V>(consumerConfig);

      List<TopicPartition> partitions = Lists.newArrayList();

      // fetch partitions for each topic
      // sort them in <topic, partition> order
      // round-robin assign the partition to splits

      try {
        for (String topic : topics) {
          for (PartitionInfo p : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(p.topic(), p.partition()));
          }
        }
      } finally {
        consumer.close();
      }

      Collections.sort(partitions, new Comparator<TopicPartition>() {
        public int compare(TopicPartition tp1, TopicPartition tp2) {
          return ComparisonChain
              .start()
              .compare(tp1.topic(), tp2.topic())
              .compare(tp1.partition(), tp2.partition())
              .result();
        }
      });

      Preconditions.checkArgument(desiredNumSplits > 0);
      Preconditions.checkState(partitions.size() > 0,
          "Could not find any partitions. Please check Kafka configuration and topic names");

      int numSplits = Math.min(desiredNumSplits, partitions.size());

      List<List<TopicPartition>> assignments = Lists.newArrayList();

      for (int i=0; i<numSplits; i++) {
        assignments.add(Lists.<TopicPartition>newArrayList());
      }

      for (int i=0; i<partitions.size(); i++) {
        assignments.get(i % numSplits).add(partitions.get(i));
      }

      List<UnboundedKafkaSource<K, V>> result = Lists.newArrayList();

      for (int i=0; i<numSplits; i++) {

        LOG.info("Partitions assigned to split {} : {}", i, Joiner.on(",").join(assignments.get(i)));

        result.add(
            new UnboundedKafkaSource<K, V>(
                this.consumerConfig,
                this.topics,
                this.keyCoder,
                this.valueCoder,
                this.timestampFn,
                this.watermarkFn,
                assignments.get(i)));
      }

      return result;
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
      return KafkaRecordCoder.of(keyCoder, valueCoder);
    }
  }

  private static class UnboundedKafkaReader<K, V>
             extends UnboundedReader<KafkaRecord<K, V>> {

    private final UnboundedKafkaSource<K, V> source;
    private KafkaConsumer<byte[], byte[]> consumer;

    // maintains state of each assigned partition
    private static class PartitionState {
      private final TopicPartition topicPartition;

      private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Iterators.emptyIterator();

      private long consumedOffset;
      // might need to keep track of per partition watermark. not decided yet about the semantics

      PartitionState(TopicPartition partition, long offset) {
        this.topicPartition = partition;
        this.consumedOffset = offset;
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

      partitionStates = ImmutableList.copyOf(Lists.transform(source.assignedPartitions,
          new Function<TopicPartition, PartitionState>() {
            public PartitionState apply(TopicPartition tp) {
              return new PartitionState(tp, -1L);
            }
        }));

      // a) verify that assigned and check-pointed partitions match exactly
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
      // read one batch of records with single consumer.poll() (may not have any records)

      ConsumerRecords<byte[], byte[]> records = consumer.poll(10);

      List<PartitionState> withRecords = Lists.newLinkedList();

      for (PartitionState pState : partitionStates) {
        List<ConsumerRecord<byte[], byte[]>> pRecords = records.records(pState.topicPartition);

        if (pRecords.size() > 0) {
          pState.recordIter = pRecords.iterator();
          withRecords.add(pState);
        }
      };

      // cycle through these partitions so that we round-robin among them while returning records
      curBatch = Iterators.cycle(withRecords);
    }

    @Override
    public boolean start() throws IOException {

      consumer = new KafkaConsumer<>(source.consumerConfig);
      consumer.assign(source.assignedPartitions);

      // seek to next offset if consumedOffset is set
      for(PartitionState p : partitionStates) {
        if (p.consumedOffset >= 0) {
          LOG.info("Reader: resuming {} at {}", p.topicPartition, p.consumedOffset + 1);
          consumer.seek(p.topicPartition, p.consumedOffset + 1);
        } else {
          LOG.info("Reader: resuming from default offset for {}", p.topicPartition);
        }
      }

      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      /* Read first record (if any). we need to loop here because :
       *  - (a) some records initially need to be skipped since they are before consumedOffset
       *  - (b) when the current batch empty, we want to readNextBatch() and then advance.
       *  - (c) curBatch is an iterator of iterators and we want to interleave the records from each.
       *    curBatch.next() might return an empty iterator.
       */
      while (true) {
        if (curBatch.hasNext()) {
          PartitionState pState = curBatch.next();

          if (!pState.recordIter.hasNext()) { // -- (c)
            pState.recordIter = Iterators.emptyIterator(); // drop ref
            curBatch.remove();
            continue;
          }

          ConsumerRecord<byte[], byte[]> rawRecord = pState.recordIter.next();
          long consumed = pState.consumedOffset;
          long offset = rawRecord.offset();

          if (consumed >= 0 && offset <= consumed) { // -- (a)
            // this can happen when compression is enabled in Kafka
            // should we check if the offset is way off from consumedOffset (say > 1M)?
            LOG.info("ignoring already consumed offset {} for {}", offset, pState.topicPartition);
            continue;
          }

          // sanity check
          if (consumed >= 0 && (offset - consumed) != 1) {
            LOG.warn("gap in offsets for {} after {}. {} records missing.",
                pState.topicPartition, consumed, offset - consumed - 1);
          }

          // apply user coders
          curRecord = new KafkaRecord<K, V>(
              rawRecord.topic(),
              rawRecord.partition(),
              rawRecord.offset(),
              decode(rawRecord.key(), source.keyCoder),
              decode(rawRecord.value(), source.valueCoder));

          curTimestamp = source.timestampFn.apply(curRecord);
          pState.consumedOffset = rawRecord.offset();

          return true;
        } else { // -- (b)
          readNextBatch();

          if (!curBatch.hasNext())
            return false;
        }
      }
    }

    private static <T> T decode(byte[] bytes, Coder<T> coder) throws IOException {
      if (bytes == null)
        return null; // is this the right thing to do?
      return coder.decode(new ExposedByteArrayInputStream(bytes), Coder.Context.OUTER);
    }

    @Override
    public Instant getWatermark() {
      // TODO : keep track of per-partition watermark
      // user provides watermark fn per partition.
      // return min of all the timestamps. what if some topics don't have any data?
      // for now we will let users handle this, we can return to it

      //XXX what should we do? why is curRecord is null? return source.timestampFn.apply(curRecord);
      LOG.info("XXX curRec is {}. curTimestamp : {}, numPartitions {} : maxOffset : {}",
          (curRecord == null) ? "null" : "not null", curTimestamp, partitionStates.size(),
          Collections.max(partitionStates, new Comparator<PartitionState>() {
            public int compare(PartitionState p1, PartitionState p2) {
              return (int) (p1.consumedOffset - p2.consumedOffset);
            }
          }));

      if (curRecord == null) // XXX TEMP
        return Instant.now();

      if (source.watermarkFn.isPresent())
        return source.watermarkFn.get().apply(curRecord);
      else
        return curTimestamp;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new KafkaCheckpointMark(
          ImmutableList.copyOf(Lists.transform(partitionStates, // avoid lazy (consumedOffset can change)
              new Function<PartitionState, PartitionMark>() {
                public PartitionMark apply(PartitionState p) {
                  return new PartitionMark(p.topicPartition, p.consumedOffset);
                }
              }
          )));
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
      return curTimestamp;
    }

    @Override
    public void close() throws IOException {
      Closeables.closeQuietly(consumer);
    }
  }

  // Builder, Source, Reader wrappers when user is only interested in Value in KafkaRecord :

  public static class ValueSourceBuilder<V> {

    private Builder<byte[], V> underlying;

    private ValueSourceBuilder(Builder<byte[], V> underlying) {
      this.underlying = underlying;
    }

    public ValueSourceBuilder<V> withBootstrapServers(String bootstrapServers) {
      return new ValueSourceBuilder<V>(underlying.withBootstrapServers(bootstrapServers));
    }

    public ValueSourceBuilder<V> withTopics(Collection<String> topics) {
      return new ValueSourceBuilder<V>(underlying.withTopics(topics));
    }

    public ValueSourceBuilder<V> withConsumerProperty(String configKey, Object configValue) {
      return new ValueSourceBuilder<V>(underlying.withConsumerProperty(configKey, configValue));
    }

    public ValueSourceBuilder<V> withConsumerProperties(Map<String, Object> configToUpdate) {
      return new ValueSourceBuilder<V>(underlying.withConsumerProperties(configToUpdate));
    }

    public ValueSourceBuilder<V> withValueCoder(Coder<V> valueCoder) {
      return new ValueSourceBuilder<V>(underlying.withValueCoder(valueCoder));
    }

    public ValueSourceBuilder<V> withTimestampFn(SerializableFunction<V, Instant> timestampFn) {
      return new ValueSourceBuilder<V>(
          underlying.withTimestampFn(unwrapKafkaAndThen(timestampFn)));
    }

    /**
     * A function to calculate watermark. When this is not set, last record timestamp is returned
     * in {@link UnboundedReader#getWatermark()}.
     */
    public ValueSourceBuilder<V> withWatermarkFn(SerializableFunction<V, Instant> watermarkFn) {
      return new ValueSourceBuilder<V>(
          underlying.withTimestampFn(unwrapKafkaAndThen(watermarkFn)));
    }

    public UnboundedSource<V, KafkaCheckpointMark> build() {
      return new UnboundedKafkaValueSource<V>((UnboundedKafkaSource<byte[], V>) underlying.build());
    }

    private static <I, O>
    SerializableFunction<KafkaRecord<byte[], I>, O> unwrapKafkaAndThen(final SerializableFunction<I, O> fn) {
      return new SerializableFunction<KafkaRecord<byte[], I>, O>() {
        public O apply(KafkaRecord<byte[], I> record) {
          return fn.apply(record.getValue());
        }
      };
    }
  }

  /**
   * Usually the users are only interested in value in KafkaRecord. This is a convenient class
   * to strip out other fields in KafkaRecord returned by UnboundedKafkaValueSource
   */
  private static class UnboundedKafkaValueSource<V> extends UnboundedSource<V, KafkaCheckpointMark> {

    private final UnboundedKafkaSource<byte[], V> underlying;

    public UnboundedKafkaValueSource(UnboundedKafkaSource<byte[], V> underlying) {
      this.underlying = underlying;
    }

    @Override
    public List<UnboundedKafkaValueSource<V>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return Lists.transform(underlying.generateInitialSplits(desiredNumSplits, options),
          new Function<UnboundedKafkaSource<byte[], V>, UnboundedKafkaValueSource<V>>() {
            public UnboundedKafkaValueSource<V> apply(UnboundedKafkaSource<byte[], V> input) {
              return new UnboundedKafkaValueSource<V>(input);
            }
          });
    }

    @Override
    public UnboundedReader<V> createReader(PipelineOptions options, KafkaCheckpointMark checkpointMark) {
      return new UnboundedKafkaValueReader<V>(this, underlying.createReader(options, checkpointMark));
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
    public Coder<V> getDefaultOutputCoder() {
      return underlying.valueCoder;
    }
  }

  private static class UnboundedKafkaValueReader<V> extends UnboundedReader<V> {

    private final UnboundedKafkaValueSource<V> source;
    private final UnboundedKafkaReader<?, V> underlying;

    public UnboundedKafkaValueReader(UnboundedKafkaValueSource<V> source,
                                     UnboundedKafkaReader<?, V> underlying) {
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
    public UnboundedKafkaValueSource<V> getCurrentSource() {
      return source;
    }

    @Override
    public V getCurrent() throws NoSuchElementException {
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

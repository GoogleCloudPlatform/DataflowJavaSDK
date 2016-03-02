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

import org.apache.kafka.clients.consumer.Consumer;
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

/**
 * Dataflow Source for consuming Kafka sources.
 *
 * <pre>
 * Usage:
 *        UnboundedSource&lt;String, ?&gt; kafkaSource = KafkaSource
 *            .&lt;String&gt;unboundedValueSourceBuilder()
 *            .withBootstrapServers("broker_1:9092,broker_2:9092)
 *            .withTopics(ImmutableList.of("topic_a", "topic_b")
 *            .withValueCoder(StringUtf8Coder.of())
 *            .withTimestampFn(timestampFn)
 *            .withWatermarkFn(watermarkFn)
 *            .build();
 *
 *        pipeline
 *          .apply(Read.from(kafkaSource).named("read_topic_a_and_b"))
 *          ....
 * </pre>
 */
public class KafkaSource {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

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
   * Similar to {@link #unboundedSourceBuilder()}, where user in only interested in value, and
   * wants to discard Kafak record key and metadata.
   *
   * @param <V> value type
   * @return {@link ValueSourceBuilder}
   */
  public static <V> ValueSourceBuilder<V> unboundedValueSourceBuilder() {
    return new ValueSourceBuilder<V>(
       new Builder<byte[], V>()
       .withKeyCoder(ByteArrayCoder.of()));
  }

  /**
   * Builds Unbounded Kafka Source.
   *
   * @param K key type
   * @param V value type
   */
  public static class Builder<K, V> {

    private List<String> topics;
    // future: let users specify subset of partitions to read.
    private Coder<K> keyCoder;
    private Coder<V> valueCoder;
    private SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn = new NowTimestampFn<>();
    private Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn =
        Optional.absent();
    private SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      kafkaConsumerFactoryFn =
        new SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>() {
          public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
            return new KafkaConsumer<>(config); // default 0.9 consumer
          }
        };

    private Map<String, Object> mutableConsumerConfig = Maps.newHashMap();

    /**
     * set of properties that are not required or don't make sense for our consumer.
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
          // default to latest offset when we are not resuming.
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
          // disable auto commit of offsets. we don't require group_id. could be enabled by user.
          ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false));
    }

    /**
     * Set Kafka bootstrap servers (alternately, set "bootstrap.servers" Consumer property).
     *
     * @param bootstrapServers Bootstrap servers for Kafka.
     * @return Builder
     */
    public Builder<K, V> withBootstrapServers(String bootstrapServers) {
      return withConsumerProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    }

    /**
     * Set Kafka topics to be consumed. This is required.
     *
     * @param topics topics to read from
     * @return Builder
     */
    public Builder<K, V> withTopics(Collection<String> topics) {
      this.topics = ImmutableList.copyOf(topics);
      return this;
    }

    /**
     * Set a {@link ConsumerConfig} configuration property.
     *
     * @param configKey configuration property name
     * @param configValue value for configuration property
     * @return Builder
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
     *
     * @param configUpdate updates to {@link ConsumerConfig}
     * @return Builder
     */
    public Builder<K, V> withConsumerProperties(Map<String, Object> configUpdate) {
      for (Entry<String, Object> e : configUpdate.entrySet()) {
        withConsumerProperty(e.getKey(), e.getValue());
      }
      return this;
    }

    /**
     * Set Coder for Key.
     *
     * @param keyCoder Coder for Key
     * @return Builder
     */
    public Builder<K, V> withKeyCoder(Coder<K> keyCoder) {
        this.keyCoder = keyCoder;
        return this;
    }

    /**
     * Set Coder for Value.
     *
     * @param valueCoder Coder for Value
     * @return Builder
     */
    public Builder<K, V> withValueCoder(Coder<V> valueCoder) {
        this.valueCoder = valueCoder;
        return this;
    }

    /**
     * A function to assign a timestamp to a record. When this is not set, processing timestamp
     * (when record is processed by {@link UnboundedReader#advance()}) is used.
     *
     * @param timestampFn Function to calculate timestamp of a record.
     * @return Builder
     */
    public Builder<K, V> withTimestampFn(
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      this.timestampFn = timestampFn;
      return this;
    }

    /**
     * A function to calculate watermark. When this is not set, last record timestamp is returned
     * in {@link UnboundedReader#getWatermark()}.
     *
     * @param watermarkFn to calculate watermark at a record.
     * @return Builder
     */
    public Builder<K, V> withWatermarkFn(
        SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {
      this.watermarkFn = Optional.of(watermarkFn);
      return this;
    }

    /**
     * A factory to create Kafka {@link Consumer} from consumer configuration.
     * Mainly used for tests.
     * @param kafkaConsumerFactoryFn function to create
     * @return
     */
    public Builder<K, V> withKafkaConsumerFactoryFn(
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn) {
      this.kafkaConsumerFactoryFn = kafkaConsumerFactoryFn;
      return this;
    }

    /**
     * Build Unbounded Kafka Source
     *
     * @return UnboundedKafkaSource
     */
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
          -1,
          consumerConfig,
          topics,
          keyCoder,
          valueCoder,
          timestampFn,
          watermarkFn,
          kafkaConsumerFactoryFn,
          ImmutableList.<TopicPartition>of() // no assigned partitions yet.
          );
    }
  }

  /** Static class, prevent instantiation. */
  private KafkaSource() {}

  private static class UnboundedKafkaSource<K, V>
      extends UnboundedSource<KafkaRecord<K, V>, KafkaCheckpointMark> {

    private final int id; // split id, mainly for debugging
    private final ImmutableMap<String, Object> consumerConfig;
    private final List<String> topics;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn;
    // would it be a good idea to pass currentTimestamp to watermarkFn?
    private final Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn;
    private
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn;
    private final List<TopicPartition> assignedPartitions;

    public UnboundedKafkaSource(
        int id,
        ImmutableMap<String, Object> consumerConfig,
        List<String> topics,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn,
        Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn,
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn,
        List<TopicPartition> assignedPartitions) {

      this.id = id;
      this.consumerConfig = consumerConfig;
      this.topics = topics;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.timestampFn = timestampFn;
      this.watermarkFn = watermarkFn;
      this.kafkaConsumerFactoryFn = kafkaConsumerFactoryFn;
      this.assignedPartitions = ImmutableList.copyOf(assignedPartitions);
    }

    @Override
    public List<UnboundedKafkaSource<K, V>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {

      Consumer<byte[], byte[]> consumer = kafkaConsumerFactoryFn.apply(consumerConfig);

      List<TopicPartition> partitions = Lists.newArrayList();

      // fetch partitions for each topic
      // sort by <topic, partition>
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

      for (int i = 0; i < numSplits; i++) {
        assignments.add(Lists.<TopicPartition>newArrayList());
      }

      for (int i = 0; i < partitions.size(); i++) {
        assignments.get(i % numSplits).add(partitions.get(i));
      }

      List<UnboundedKafkaSource<K, V>> result = Lists.newArrayList();

      for (int i = 0; i < numSplits; i++) {
        List<TopicPartition> assignedToSplit = assignments.get(i);

        LOG.info("Partitions assigned to split {} (total {}): {}",
            i, assignedToSplit.size(), Joiner.on(",").join(assignedToSplit));

        result.add(new UnboundedKafkaSource<K, V>(
            i,
            this.consumerConfig,
            this.topics,
            this.keyCoder,
            this.valueCoder,
            this.timestampFn,
            this.watermarkFn,
            this.kafkaConsumerFactoryFn,
            assignedToSplit));
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

    // maintains state of each assigned partition (buffered records and consumed offset)
    private static class PartitionState {
      private final TopicPartition topicPartition;
      private long consumedOffset;
      private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Iterators.emptyIterator();

      PartitionState(TopicPartition partition, long offset) {
        this.topicPartition = partition;
        this.consumedOffset = offset;
      }
    }

    private final UnboundedKafkaSource<K, V> source;
    private final String name;
    private Consumer<byte[], byte[]> consumer;
    private final List<PartitionState> partitionStates;
    private KafkaRecord<K, V> curRecord;
    private Instant curTimestamp;
    private Iterator<PartitionState> curBatch = Iterators.emptyIterator();

    /** watermark before any records have been read. */
    private static Instant initialWatermark = new Instant(Long.MIN_VALUE);

    public String toString() {
      return name;
    }

    public UnboundedKafkaReader(
        UnboundedKafkaSource<K, V> source,
        @Nullable KafkaCheckpointMark checkpointMark) {

      this.source = source;
      this.name = "Reader-" + source.id;

      partitionStates = ImmutableList.copyOf(Lists.transform(source.assignedPartitions,
          new Function<TopicPartition, PartitionState>() {
            public PartitionState apply(TopicPartition tp) {
              return new PartitionState(tp, -1L);
            }
        }));

      // a) verify that assigned and check-pointed partitions match exactly
      // b) set consumed offsets

      if (checkpointMark != null) {
        // set consumed offset

        Preconditions.checkState(
            checkpointMark.getPartitions().size() == source.assignedPartitions.size(),
            "checkPointMark and assignedPartitions should match");
        // we could consider allowing a mismatch, though it is not expected in current Dataflow

        for (int i = 0; i < source.assignedPartitions.size(); i++) {
          PartitionMark ckptMark = checkpointMark.getPartitions().get(i);
          TopicPartition assigned = source.assignedPartitions.get(i);

          Preconditions.checkState(ckptMark.getTopicPartition().equals(assigned),
              "checkpointed partition %s and assinged partition %s don't match at position %d",
              ckptMark.getTopicPartition(), assigned, i);

          partitionStates.get(i).consumedOffset = ckptMark.getOffset();
        }
      }
    }

    private void readNextBatch(boolean isFirstFetch) {
      // read one batch of records with single consumer.poll() (may not return any records)

      // Use a longer timeout for first fetch. Kafka consumer seems to do better with poll() with
      // longer timeout initially. Looks like it does not handle initial connection setup properly
      // with short polls and backoff policy in Dataflow might be making things worse for
      // this case. In my tests it took ~5 seconds before first record was read with this
      // hack and 20-30 seconds with out.
      long timeoutMillis = isFirstFetch ? 4000 : 100;
      ConsumerRecords<byte[], byte[]> records = consumer.poll(timeoutMillis);

      List<PartitionState> nonEmpty = Lists.newLinkedList();

      for (PartitionState p : partitionStates) {
        p.recordIter = records.records(p.topicPartition).iterator();
        if (p.recordIter.hasNext()) {
          nonEmpty.add(p);
        }
      }

      // cycle through the partitions in order to interleave records from each.
      curBatch = Iterators.cycle(nonEmpty);
    }

    @Override
    public boolean start() throws IOException {
      consumer = source.kafkaConsumerFactoryFn.apply(source.consumerConfig);
      consumer.assign(source.assignedPartitions);

      // seek to next offset if consumedOffset is set
      for (PartitionState p : partitionStates) {
        if (p.consumedOffset >= 0) {
          LOG.info("{}: resuming {} at {}", name, p.topicPartition, p.consumedOffset + 1);
          consumer.seek(p.topicPartition, p.consumedOffset + 1);
        } else {
          LOG.info("{} : resuming {} at default offset", name, p.topicPartition);
        }
      }

      readNextBatch(true);
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      /* Read first record (if any). we need to loop here because :
       *  - (a) some records initially need to be skipped since they are before consumedOffset
       *  - (b) when the current batch empty, we want to readNextBatch() and then advance.
       *  - (c) curBatch is an iterator of iterators. we interleave the records from each.
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

          // apply user coders
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

          if (curRecord == null) {
            LOG.info("{} : first record offset {}", name, offset);
          }

          // apply user coders. might want to allow skipping records that fail in coders.
          curRecord = new KafkaRecord<K, V>(
              rawRecord.topic(),
              rawRecord.partition(),
              rawRecord.offset(),
              decode(rawRecord.key(), source.keyCoder),
              decode(rawRecord.value(), source.valueCoder));

          curTimestamp = source.timestampFn.apply(curRecord);
          pState.consumedOffset = offset;
          return true;

        } else { // -- (b)
          readNextBatch(false);

          if (!curBatch.hasNext()) {
            return false;
          }
        }
      }
    }

    private static <T> T decode(byte[] bytes, Coder<T> coder) throws IOException {
      if (bytes == null) {
        return null; // is this the right thing to do?
      }
      return coder.decode(new ExposedByteArrayInputStream(bytes), Coder.Context.OUTER);
    }

    @Override
    public Instant getWatermark() {
      if (curRecord == null) {
        LOG.warn("{} : getWatermark() : no records have been read yet.", name);
        return initialWatermark;
      }

      return source.watermarkFn.isPresent() ?
          source.watermarkFn.get().apply(curRecord) : curTimestamp;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new KafkaCheckpointMark(ImmutableList.copyOf(// avoid lazy (consumedOffset can change)
          Lists.transform(partitionStates,
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
      // should we delay updating consumed offset till this point? Mostly not required.
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

  // Builder, Source, and Reader wrappers. Often user is only interested in Value in KafkaRecord :

  /**
   * Builder for Kafka Source where user is not interested in Kafka metadata and key for a record,
   * but just the value.
   */
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
     *
     * @param watermarkFn to calculate watermark at a record.
     * @return Builder
     */
    public ValueSourceBuilder<V> withWatermarkFn(SerializableFunction<V, Instant> watermarkFn) {
      return new ValueSourceBuilder<V>(
          underlying.withTimestampFn(unwrapKafkaAndThen(watermarkFn)));
    }

    /**
     * A factory to create Kafka {@link Consumer} from consumer configuration.
     * Mainly used for tests.
     * @param kafkaConsumerFactoryFn function to create
     * @return
     */
    public ValueSourceBuilder<V> withKafkaConsumerFactoryFn(
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn) {
      return new ValueSourceBuilder<V>(
          underlying.withKafkaConsumerFactoryFn(kafkaConsumerFactoryFn));
    }

    public UnboundedSource<V, KafkaCheckpointMark> build() {
      return new UnboundedKafkaValueSource<V>((UnboundedKafkaSource<byte[], V>) underlying.build());
    }

    private static <InT, OutT>
    SerializableFunction<KafkaRecord<byte[], InT>, OutT> unwrapKafkaAndThen(
        final SerializableFunction<InT, OutT> fn) {
      return new SerializableFunction<KafkaRecord<byte[], InT>, OutT>() {
        public OutT apply(KafkaRecord<byte[], InT> record) {
          return fn.apply(record.getValue());
        }
      };
    }
  }

  /**
   * Usually the users are only interested in value in KafkaRecord. This is a convenient class
   * to strip out other fields in KafkaRecord returned by UnboundedKafkaValueSource
   */
  private static class UnboundedKafkaValueSource<V>
                                          extends UnboundedSource<V, KafkaCheckpointMark> {

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
    public UnboundedReader<V> createReader(
        PipelineOptions options, KafkaCheckpointMark checkpointMark) {
      return new UnboundedKafkaValueReader<V>(this,
          underlying.createReader(options, checkpointMark));
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

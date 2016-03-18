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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.repackaged.com.google.common.annotations.VisibleForTesting;
import com.google.cloud.dataflow.contrib.kafka.KafkaCheckpointMark.PartitionMark;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.Read.Unbounded;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.ExposedByteArrayInputStream;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Dataflow support for consuming Kafka topics as {@link Unbounded} sources.
 *
 * <p>
 * The Kafka partitions
 * are evenly distributed among splits (workers). Dataflow checkpointing is fully supported and
 * each split can resume from previous checkpoint. See
 * {@link UnboundedKafkaSource#generateInitialSplits(int, PipelineOptions)} for more details on
 * splits and checkpoint support.
 *
 * <pre>{@code
 * Usage:
 *   pipeline
 *     .apply("read_topic_a_and_b",
 *        KafkaIO.read()
 *         .withBootstrapServers("broker_1:9092,broker_2:9092")
 *         .withTopics(ImmutableList.of("topic_a", "topic_b")) // or withTopicPartitions(List<>)
 *         .withValueCoder(StringUtf8Coder.of())
 *         .withTimestampFn(timestampFn) // optional
 *         .withWatermarkFn(watermarkFn)) // optional
 *     .apply(Values.<String>create()) // discard keys
 *     ...
 *}</pre>
 */
public class KafkaIO {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaIO.class);

  private static class NowTimestampFn<T> implements SerializableFunction<T, Instant> {
    @Override
    public Instant apply(T input) {
      return Instant.now();
    }
  }

  private static class IdentityFn<T> implements SerializableFunction<T, T> {
    @Override
    public T apply(T input) {
      return input;
    }
  }

  /**
   * Creates and uninitialized {@link Read} {@link PTransform}. Before use, basic Kafka
   * configuration should set with {@link Read#withBootstrapServers(String)} and
   * {@link Read#withTopics(List)}. Other optional settings include key and value coders,
   * custom timestamp and watermark functions. Additionally, {@link Read#withMetadata()} provides
   * access to Kafka metadata for each record (topic name, partition, offset).
   */
  public static Read<byte[], byte[]> read() {
    return new Read<byte[], byte[]>(
        new ArrayList<String>(),
        new ArrayList<TopicPartition>(),
        ByteArrayCoder.of(),
        ByteArrayCoder.of(),
        null,
        null,
        Read.kafka9ConsumerFactory,
        Read.defaultConsumerProperties,
        Long.MAX_VALUE,
        null);
  }

  /**
   * A {@link PTransform} to read from Kafka topics. See {@link KafkaIO#read()} for more
   * information on configuration.
   */
  public static class Read<K, V> extends PTransform<PInput, PCollection<KV<K, V>>> {

    private final List<String> topics;
    private final List<TopicPartition> topicPartitions; // mutually exclusive with topics
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    @Nullable private final SerializableFunction<KV<K, V>, Instant> timestampFn;
    @Nullable private final SerializableFunction<KV<K, V>, Instant> watermarkFn;
    private final
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn;
    private final Map<String, Object> consumerConfig;
    private final long maxNumRecords; // bounded read, mainly for testing
    private final Duration maxReadTime; // bounded read, mainly for testing

    /**
     * A set of properties that are not required or don't make sense for our consumer.
     */
    private static final Map<String, String> ignoredConsumerProperties = ImmutableMap.of(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "Set keyDecoderFn instead",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "Set valueDecoderFn instead"
        // "group.id", "enable.auto.commit", "auto.commit.interval.ms" :
        //     lets allow these, applications can have better resume point for restarts.
        );

    // set config defaults
    private static final Map<String, Object> defaultConsumerProperties =
        ImmutableMap.<String, Object>of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(),

            // Use large receive buffer. Once KAFKA-3135 is fixed, this _may_ not be required.
            // with default value of of 32K, It takes multiple seconds between successful polls.
            // All the consumer work is done inside poll(), with smaller send buffer size, it
            // takes many polls before a 1MB chunk from the server is fully read. In my testing
            // about half of the time select() inside kafka consumer waited for 20-30ms, though
            // the server had lots of data in tcp send buffers on its side. Compared to default,
            // this setting increased throughput increased by many fold (3-4x).
            ConsumerConfig.RECEIVE_BUFFER_CONFIG, 512 * 1024,

            // default to latest offset when we are not resuming.
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
            // disable auto commit of offsets. we don't require group_id. could be enabled by user.
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    // default Kafka 0.9 Consumer supplier. static variable to avoid capturing 'this'
    private static SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
      kafka9ConsumerFactory =
        new SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>() {
          public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
            return new KafkaConsumer<>(config);
          }
        };

    private Read(
        List<String> topics,
        List<TopicPartition> topicPartitions,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        @Nullable SerializableFunction<KV<K, V>, Instant> timestampFn,
        @Nullable SerializableFunction<KV<K, V>, Instant> watermarkFn,
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
        Map<String, Object> consumerConfig,
        long maxNumRecords,
        @Nullable Duration maxReadTime) {

      this.topics = topics;
      this.topicPartitions = topicPartitions;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.timestampFn = timestampFn;
      this.watermarkFn = watermarkFn;
      this.consumerFactoryFn = consumerFactoryFn;
      this.consumerConfig = consumerConfig;
      this.maxNumRecords = maxNumRecords;
      this.maxReadTime = maxReadTime;
    }

    /**
     * Returns a new {@link Read} with Kafka consumer pointing to {@code bootstrapServers}.
     */
    public Read<K, V> withBootstrapServers(String bootstrapServers) {
      return updateConsumerProperties(
          ImmutableMap.<String, Object>of(
              ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    /**
     * Returns a new {@link Read} that reads from the topics. All the partitions are from each
     * of the topics is read.
     * See {@link UnboundedKafkaSource#generateInitialSplits(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopics(List<String> topics) {
      checkState(topicPartitions.isEmpty(), "Only topics or topicPartitions can be set, not both");

      return new Read<K, V>(ImmutableList.copyOf(topics), topicPartitions, keyCoder, valueCoder,
          timestampFn, watermarkFn, consumerFactoryFn, consumerConfig, maxNumRecords, maxReadTime);
    }

    /**
     * Returns a new {@link Read} that reads from the partitions. This allows reading only a subset
     * of partitions for one or more topics when (if ever) needed.
     * See {@link UnboundedKafkaSource#generateInitialSplits(int, PipelineOptions)} for description
     * of how the partitions are distributed among the splits.
     */
    public Read<K, V> withTopicPartitions(List<TopicPartition> topicPartitions) {
      checkState(topics.isEmpty(), "Only topics or topicPartitions can be set, not both");

      return new Read<K, V>(topics, ImmutableList.copyOf(topicPartitions), keyCoder, valueCoder,
          timestampFn, watermarkFn, consumerFactoryFn, consumerConfig, maxNumRecords, maxReadTime);
    }

    /**
     * Returns a new {@link Read} with {@link Coder} for key bytes.
     * <p> Since this changes the type for key, settings that depend on the type
     * ({@link #withTimestampFn(SerializableFunction)} and
     *  {@link #withWatermarkFn(SerializableFunction)}) make sense only after the coders are set.
     */
    public <KeyT> Read<KeyT, V> withKeyCoder(Coder<KeyT> keyCoder) {
      checkState(timestampFn == null, "Set timestampFn after setting key and value coders");
      checkState(watermarkFn == null, "Set watermarkFn after setting key and value coders");
      return new Read<KeyT, V>(topics, topicPartitions, keyCoder, valueCoder, null, null,
          consumerFactoryFn, consumerConfig, maxNumRecords, maxReadTime);
    }

    /**
     * Returns a new {@link Read} with {@link Coder} for value bytes.
     * <p> Since this changes the type for key, settings that depend on the type
     * ({@link #withTimestampFn(SerializableFunction)} and
     *  {@link #withWatermarkFn(SerializableFunction)}) make sense only after the coders are set.
     */
    public <ValueT> Read<K, ValueT> withValueCoder(Coder<ValueT> valueCoder) {
      checkState(timestampFn == null, "Set timestampFn after setting key and value coders");
      checkState(watermarkFn == null, "Set watermarkFn after setting key and value coders");
      return new Read<K, ValueT>(topics, topicPartitions, keyCoder, valueCoder, null, null,
          consumerFactoryFn, consumerConfig, maxNumRecords, maxReadTime);
    }

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     */
    public Read<K, V> withTimestampFn(SerializableFunction<KV<K, V>, Instant> timestampFn) {
      checkNotNull(timestampFn);
      return new Read<K, V>(topics, topicPartitions, keyCoder, valueCoder,
          timestampFn, watermarkFn, consumerFactoryFn, consumerConfig, maxNumRecords, maxReadTime);
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp
     * @see #withTimestampFn(SerializableFunction)
     */
    public Read<K, V> withWatermarkFn(SerializableFunction<KV<K, V>, Instant> watermarkFn) {
      checkNotNull(watermarkFn);
      return new Read<K, V>(topics, topicPartitions, keyCoder, valueCoder,
          timestampFn, watermarkFn, consumerFactoryFn, consumerConfig, maxNumRecords, maxReadTime);
    }

    /**
     * A factory to create Kafka {@link Consumer} from consumer configuration.
     * Mainly used for tests. Default is {@link KafkaConsumer}.
     */
    public Read<K, V> withConsumerFactoryFn(
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn) {
      return new Read<K, V>(topics, topicPartitions, keyCoder, valueCoder,
          timestampFn, watermarkFn, consumerFactoryFn, consumerConfig, maxNumRecords, maxReadTime);
    }

    /**
     * Update consumer configuration with new properties.
     */
    public Read<K, V> updateConsumerProperties(Map<String, Object> configUpdates) {
      for (String key : configUpdates.keySet()) {
        checkArgument(!ignoredConsumerProperties.containsKey(key),
            "No need to configure '%s'. %s", key, ignoredConsumerProperties.get(key));
      }

      Map<String, Object> config = new HashMap<>(consumerConfig);
      config.putAll(configUpdates);

      return new Read<K, V>(topics, topicPartitions, keyCoder, valueCoder, timestampFn,
          watermarkFn, consumerFactoryFn, config, maxNumRecords, maxReadTime);
    }

    /**
     * Similar to {@link com.google.cloud.dataflow.sdk.io.Read.Unbounded#withMaxNumRecords(long)}.
     * Mainly used for tests and demo applications.
     */
    public Read<K, V> withMaxNumRecords(long maxNumRecords) {
      return new Read<K, V>(topics, topicPartitions, keyCoder, valueCoder, timestampFn,
          watermarkFn, consumerFactoryFn, consumerConfig, maxNumRecords, null);
    }

    /**
     * Similar to
     * {@link com.google.cloud.dataflow.sdk.io.Read.Unbounded#withMaxReadTime(Duration)}.
     * Mainly used for tests and demo
     * applications.
     */
    public Read<K, V> withMaxReadTime(Duration maxReadTime) {
      return new Read<K, V>(topics, topicPartitions, keyCoder, valueCoder, timestampFn,
          watermarkFn, consumerFactoryFn, consumerConfig, Long.MAX_VALUE, maxReadTime);
    }

    /**
     * A read transform that includes Kafka metadata along with key and value.
     * @see KafkaRecord
     */
    public ReadWithMetadata<K, V> withMetadata() {
      return new ReadWithMetadata<K, V>(this,
          timestampFn != null ? unwrapKafkaAndThen(timestampFn) : null,
          watermarkFn != null ? unwrapKafkaAndThen(watermarkFn) : null);
    }

    // utility method to convert KafkRecord<K, V> to user KV<K, V> before applying user functions
    private static <KeyT, ValueT, OutT> SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT>
      unwrapKafkaAndThen(final SerializableFunction<KV<KeyT, ValueT>, OutT> fn) {
        return new SerializableFunction<KafkaRecord<KeyT, ValueT>, OutT>() {
          public OutT apply(KafkaRecord<KeyT, ValueT> record) {
            return fn.apply(record.getKV());
          }
        };
      }

    /**
     * Creates an {@link UnboundedSource<KafkaRecord<K, V>, ?>} with the configuration in
     * {@link ReadWithMetadata}. Primary use case is unit tests, should not be used in an
     * application.
     */
    @VisibleForTesting
    UnboundedKafkaSource<K, V, KV<K, V>> makeSource() {
      return new UnboundedKafkaSource<K, V, KV<K, V>>(
          -1,
          topics,
          topicPartitions,
          keyCoder,
          valueCoder,
          KvCoder.of(keyCoder, valueCoder),
          unwrapKafkaAndThen(new IdentityFn<KV<K, V>>()),
          timestampFn == null ? null : unwrapKafkaAndThen(timestampFn),
          Optional.fromNullable(watermarkFn == null ? null : unwrapKafkaAndThen(watermarkFn)),
          consumerFactoryFn,
          consumerConfig);
    }

    @Override
    public PCollection<KV<K, V>> apply(PInput input) {
      return applyHelper(input, makeSource());
    }

    /**
     * Handles unbounded source to bounded conversion if maxNumRecords or maxReadTime is set.
     */
    <T> PCollection<T> applyHelper(PInput input, UnboundedSource<T, ?> source) {
      Unbounded<T> unbounded = com.google.cloud.dataflow.sdk.io.Read.from(source);
      PTransform<PInput, PCollection<T>> transform = unbounded;

      if (maxNumRecords < Long.MAX_VALUE) {
        transform = unbounded.withMaxNumRecords(maxNumRecords);
      } else if (maxReadTime != null) {
        transform = unbounded.withMaxReadTime(maxReadTime);
      }

      return input.getPipeline().apply(transform);
    }
  }

  /**
   * Similar to {@link Read}, except that the transform returns a PCollection of
   * {@link KafkaRecord} which includes Kafka metadata : topic name, partition, offset.
   */
  public static class ReadWithMetadata<K, V>
                      extends PTransform<PInput, PCollection<KafkaRecord<K, V>>> {

    private final Read<K, V> kvRead;
    @Nullable private final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn;
    @Nullable private final SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn;

    ReadWithMetadata(
        Read<K, V> kvRead,
        @Nullable SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn,
        @Nullable SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {

      this.kvRead = kvRead;
      this.timestampFn = timestampFn;
      this.watermarkFn = watermarkFn;
    }

    // Interface Note:
    // Instead of repeating many of the builder methods ('withTopics()' etc) in Reader, we expect
    // the user to set all those before Reader.withMetaData(). we still need to let users
    // override timestamp functions (in cases where these functions need metadata).

    /**
     * A function to assign a timestamp to a record. Default is processing timestamp.
     */
    public ReadWithMetadata<K, V> withTimestampFn(
        SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn) {
      checkNotNull(timestampFn);
      return new ReadWithMetadata<K, V>(kvRead, timestampFn, watermarkFn);
    }

    /**
     * A function to calculate watermark after a record. Default is last record timestamp
     * @see #withTimestampFn(SerializableFunction)
     */
    public ReadWithMetadata<K, V> withWatermarkFn(
        SerializableFunction<KafkaRecord<K, V>, Instant> watermarkFn) {
      checkNotNull(watermarkFn);
      return new ReadWithMetadata<K, V>(kvRead, timestampFn, watermarkFn);
    }

    /**
     * Creates an {@link UnboundedSource<KafkaRecord<K, V>, ?>} with the configuration in
     * {@link ReadWithMetadata}. Primary use case is unit tests, should not be used in an
     * application.
     */
    @VisibleForTesting
    UnboundedKafkaSource<K, V, KafkaRecord<K, V>> makeSource() {
      return new UnboundedKafkaSource<>(
          -1,
          kvRead.topics,
          kvRead.topicPartitions,
          kvRead.keyCoder,
          kvRead.valueCoder,
          KafkaRecordCoder.of(kvRead.keyCoder, kvRead.valueCoder),
          new IdentityFn<KafkaRecord<K, V>>(),
          timestampFn,
          Optional.fromNullable(watermarkFn),
          kvRead.consumerFactoryFn,
          kvRead.consumerConfig);
    }

    @Override
    public PCollection<KafkaRecord<K, V>> apply(PInput input) {
      return kvRead.applyHelper(input, makeSource());
    }
  }

  /** Static class, prevent instantiation. */
  private KafkaIO() {}

  private static class UnboundedKafkaSource<K, V, T>
      extends UnboundedSource<T, KafkaCheckpointMark> {

    private final int id; // split id, mainly for debugging
    private final List<String> topics;
    private final List<TopicPartition> assignedPartitions;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Coder<T> defaultOutputCoder;
    private final SerializableFunction<KafkaRecord<K, V>, T> converterFn; // covert to userTuype.
    private final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn;
    // would it be a good idea to pass currentTimestamp to watermarkFn?
    private final Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn;
    private
      SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn;
    private final Map<String, Object> consumerConfig;

    public UnboundedKafkaSource(
        int id,
        List<String> topics,
        List<TopicPartition> assignedPartitions,
        Coder<K> keyCoder,
        Coder<V> valueCoder,
        Coder<T> defaultOutputCoder,
        SerializableFunction<KafkaRecord<K, V>, T> converterFn,
        @Nullable SerializableFunction<KafkaRecord<K, V>, Instant> timestampFn,
        Optional<SerializableFunction<KafkaRecord<K, V>, Instant>> watermarkFn,
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> consumerFactoryFn,
        Map<String, Object> consumerConfig) {

      this.id = id;
      this.assignedPartitions = assignedPartitions;
      this.topics = topics;
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.defaultOutputCoder = defaultOutputCoder;
      this.converterFn = converterFn;
      this.timestampFn =
          (timestampFn == null ? new NowTimestampFn<KafkaRecord<K, V>>() : timestampFn);
      this.watermarkFn = watermarkFn;
      this.consumerFactoryFn = consumerFactoryFn;
      this.consumerConfig = consumerConfig;
    }

    /**
     * The partitions are evenly distributed among the splits. The number of splits returned is
     * {@code min(desiredNumSplits, totalNumPartitions)}, though better not to depend on the exact
     * count.
     *
     * <p> It is important to assign the partitions deterministically so that we can support
     * resuming a split from last checkpoint. The Kafka partitions are sorted by
     * {@code <topic, partition>} and then assigned to splits in round-robin order.
     */
    @Override
    public List<UnboundedKafkaSource<K, V, T>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {

      List<TopicPartition> partitions = new ArrayList<>(assignedPartitions);

      // (a) fetch partitions for each topic
      // (b) sort by <topic, partition>
      // (c) round-robin assign the partitions to splits

      if (partitions.isEmpty()) {
        try (Consumer<?, ?> consumer = consumerFactoryFn.apply(consumerConfig)) {
          for (String topic : topics) {
            for (PartitionInfo p : consumer.partitionsFor(topic)) {
              partitions.add(new TopicPartition(p.topic(), p.partition()));
            }
          }
        }
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

      checkArgument(desiredNumSplits > 0);
      checkState(partitions.size() > 0,
          "Could not find any partitions. Please check Kafka configuration and topic names");

      int numSplits = Math.min(desiredNumSplits, partitions.size());
      List<List<TopicPartition>> assignments = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        assignments.add(new ArrayList<TopicPartition>());
      }
      for (int i = 0; i < partitions.size(); i++) {
        assignments.get(i % numSplits).add(partitions.get(i));
      }

      List<UnboundedKafkaSource<K, V, T>> result = new ArrayList<>(numSplits);

      for (int i = 0; i < numSplits; i++) {
        List<TopicPartition> assignedToSplit = assignments.get(i);

        LOG.info("Partitions assigned to split {} (total {}): {}",
            i, assignedToSplit.size(), Joiner.on(",").join(assignedToSplit));

        result.add(new UnboundedKafkaSource<K, V, T>(
            i,
            this.topics,
            assignedToSplit,
            this.keyCoder,
            this.valueCoder,
            this.defaultOutputCoder,
            this.converterFn,
            this.timestampFn,
            this.watermarkFn,
            this.consumerFactoryFn,
            this.consumerConfig));
      }

      return result;
    }

    @Override
    public UnboundedKafkaReader<K, V, T> createReader(PipelineOptions options,
                                                   KafkaCheckpointMark checkpointMark) {
      if (assignedPartitions.isEmpty()) {
        LOG.warn("hack: working around DirectRunner issue. It does not generateSplits()");
        // generate single split and return reader from it.
        try {
          return new UnboundedKafkaReader<K, V, T>(
              generateInitialSplits(1, options).get(0), checkpointMark);
        } catch (Exception e) {
          Throwables.propagate(e);
        }
      }
      return new UnboundedKafkaReader<K, V, T>(this, checkpointMark);
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
      checkNotNull(consumerConfig.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
          "Kafka bootstrap servers should be set");
      checkArgument(topics.size() > 0 || assignedPartitions.size() > 0,
          "Kafka topics or topic_partitions are required");
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return defaultOutputCoder;
    }
  }

  private static class UnboundedKafkaReader<K, V, T> extends UnboundedReader<T> {

    private final UnboundedKafkaSource<K, V, T> source;
    private final String name;
    private Consumer<byte[], byte[]> consumer;
    private final List<PartitionState> partitionStates;
    private KafkaRecord<K, V> curRecord;
    private Instant curTimestamp;
    private Iterator<PartitionState> curBatch = Collections.emptyIterator();

    // Use a separate thread to read Kafka messages. Kafka Consumer does all its work including
    // network I/O inside poll(). Polling only inside #advance(), especially with a small timeout
    // like 100 milliseconds does not work well. This along with large receive buffer for
    // consumer achieved best throughput in tests (see `defaultConsumerProperties`).
    private final ExecutorService consumerPollThread = Executors.newSingleThreadExecutor();
    private final SynchronousQueue<ConsumerRecords<byte[], byte[]>> availableRecordsQueue =
        new SynchronousQueue<>();
    private volatile boolean closed = false;

    // Backlog support :
    // Kafka consumer does not have an API to fetch latest offset for topic. We need to seekToEnd()
    // then look at position(). Use another consumer to do this so that the primary consumer does
    // not need to be interrupted. The latest offsets are fetched periodically on another thread.
    // This is still a hack. There could be unintended side effects, e.g. if user enabled offset
    // auto commit in consumer config, this could interfere with the primary consumer (we will
    // handle this particular problem). We might have to make this optional.
    private Consumer<byte[], byte[]> offsetConsumer;
    private final ScheduledExecutorService offsetFetcherThread =
        Executors.newSingleThreadScheduledExecutor();
    static private final int OFFSET_UPDATE_INTERVAL_MINUTES = 5;

    /** watermark before any records have been read. */
    private static Instant initialWatermark = new Instant(Long.MIN_VALUE);

    public String toString() {
      return name;
    }

    // maintains state of each assigned partition (buffered records, consumed offset, etc)
    private static class PartitionState {
      private final TopicPartition topicPartition;
      private long consumedOffset;
      private long latestOffset;
      private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = Collections.emptyIterator();

      // simple moving average for size of each record in bytes
      private double avgRecordSize = 0;
      private static final int movingAvgWindow = 1000; // very roughly avg of last 1000 elements


      PartitionState(TopicPartition partition, long offset) {
        this.topicPartition = partition;
        this.consumedOffset = offset;
        this.latestOffset = -1;
      }

      // update consumedOffset and avgRecordSize
      void recordConsumed(long offset, int size) {
        consumedOffset = offset;

        // this is always updated from single thread. probably not worth making it an AtomicDouble
        if (avgRecordSize <= 0) {
          avgRecordSize = size;
        } else {
          // initially, first record heavily contributes to average.
          avgRecordSize += ((size - avgRecordSize) / movingAvgWindow);
        }
      }

      synchronized void setLatestOffset(long latestOffset) {
        this.latestOffset = latestOffset;
      }

      synchronized long approxBacklogInBytes() {
        // Note that is an an estimate of uncompressed backlog.
        // Messages on Kafka might be comressed.
        if (latestOffset < 0 || consumedOffset < 0) {
          return UnboundedReader.BACKLOG_UNKNOWN;
        }
        if (latestOffset <= consumedOffset || consumedOffset < 0) {
          return 0;
        }
        return (long) ((latestOffset - consumedOffset - 1) * avgRecordSize);
      }
    }

    public UnboundedKafkaReader(
        UnboundedKafkaSource<K, V, T> source,
        @Nullable KafkaCheckpointMark checkpointMark) {

      this.source = source;
      this.name = "Reader-" + source.id;

      partitionStates = ImmutableList.copyOf(Lists.transform(source.assignedPartitions,
          new Function<TopicPartition, PartitionState>() {
            public PartitionState apply(TopicPartition tp) {
              return new PartitionState(tp, -1L);
            }
        }));

      if (checkpointMark != null) {
        // a) verify that assigned and check-pointed partitions match exactly
        // b) set consumed offsets

        checkState(checkpointMark.getPartitions().size() == source.assignedPartitions.size(),
            "checkPointMark and assignedPartitions should match");
        // we could consider allowing a mismatch, though it is not expected in current Dataflow

        for (int i = 0; i < source.assignedPartitions.size(); i++) {
          PartitionMark ckptMark = checkpointMark.getPartitions().get(i);
          TopicPartition assigned = source.assignedPartitions.get(i);

          checkState(ckptMark.getTopicPartition().equals(assigned),
              "checkpointed partition %s and assinged partition %s don't match at position %d",
              ckptMark.getTopicPartition(), assigned, i);

          partitionStates.get(i).consumedOffset = ckptMark.getOffset();
        }
      }
    }

    private void consumerPollLoop() {
      // Read in a loop and enqueue the batch of records, if any, to availableRecordsQueue
      while (!closed) {
        try {
          ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
          if (!records.isEmpty()) {
            availableRecordsQueue.put(records); // blocks until dequeued.
          }
        } catch (InterruptedException e) {
          LOG.warn(this + " consumer thread is interrupted", e); // not expected
          break;
        } catch (WakeupException e) {
          break;
        }
      }

      LOG.info("{} : Returning from consumer pool loop", this);
    }

    private void nextBatch() {
      curBatch = Collections.emptyIterator();

      ConsumerRecords<byte[], byte[]> records;
      try {
        records = availableRecordsQueue.poll(10, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Unexpected", e);
        return;
      }

      if (records == null) {
        return;
      }

      List<PartitionState> nonEmpty = new LinkedList<>();

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
      consumer = source.consumerFactoryFn.apply(source.consumerConfig);
      consumer.assign(source.assignedPartitions);

      // seek to consumedOffset + 1 if it is set
      for (PartitionState p : partitionStates) {
        if (p.consumedOffset >= 0) {
          LOG.info("{}: resuming {} at {}", name, p.topicPartition, p.consumedOffset + 1);
          consumer.seek(p.topicPartition, p.consumedOffset + 1);
        } else {
          LOG.info("{} : resuming {} at default offset", name, p.topicPartition);
        }
      }

      // start consumer read loop.
      // Note that consumer is not thread safe, should not accessed out side consumerPollLoop()
      consumerPollThread.submit(
          new Runnable() {
            public void run() {
              consumerPollLoop();
            }
          });

      // offsetConsumer setup :

      // override client_id and auto_commit so that it does not interfere with main consumer.
      String offsetConsumerId = String.format("%s_offset_consumer_%d_%s", name,
          (new Random()).nextInt(Integer.MAX_VALUE),
          source.consumerConfig.getOrDefault(ConsumerConfig.CLIENT_ID_CONFIG, "none"));
      Map<String, Object> offsetConsumerConfig = new HashMap<>(source.consumerConfig);
      offsetConsumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, offsetConsumerId);
      offsetConsumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

      offsetConsumer = source.consumerFactoryFn.apply(offsetConsumerConfig);
      offsetConsumer.assign(source.assignedPartitions);

      offsetFetcherThread.scheduleAtFixedRate(
          new Runnable() {
            public void run() {
              updateLatestOffsets();
            }
          }, 0, OFFSET_UPDATE_INTERVAL_MINUTES, TimeUnit.SECONDS);

      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      /* Read first record (if any). we need to loop here because :
       *  - (a) some records initially need to be skipped if they are before consumedOffset
       *  - (b) if curBatch is empty, we want to fetch next batch and then advance.
       *  - (c) curBatch is an iterator of iterators. we interleave the records from each.
       *        curBatch.next() might return an empty iterator.
       */
      while (true) {
        if (curBatch.hasNext()) {
          PartitionState pState = curBatch.next();

          if (!pState.recordIter.hasNext()) { // -- (c)
            pState.recordIter = Collections.emptyIterator(); // drop ref
            curBatch.remove();
            continue;
          }

          ConsumerRecord<byte[], byte[]> rawRecord = pState.recordIter.next();
          long consumed = pState.consumedOffset;
          long offset = rawRecord.offset();

          if (consumed >= 0 && offset <= consumed) { // -- (a)
            // this can happen when compression is enabled in Kafka (seems to be fixed in 0.10)
            // should we check if the offset is way off from consumedOffset (say > 1M)?
            LOG.warn("ignoring already consumed offset {} for {}", offset, pState.topicPartition);
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

          // apply user coders. might want to allow skipping records that fail to decode.
          curRecord = new KafkaRecord<K, V>(
              rawRecord.topic(),
              rawRecord.partition(),
              rawRecord.offset(),
              decode(rawRecord.key(), source.keyCoder),
              decode(rawRecord.value(), source.valueCoder));

          curTimestamp = source.timestampFn.apply(curRecord);

          int recordSize = (rawRecord.key() == null ? 0 : rawRecord.key().length) +
              (rawRecord.value() == null ? 0 : rawRecord.value().length);
          pState.recordConsumed(offset, recordSize);
          return true;

        } else { // -- (b)
          nextBatch();

          if (!curBatch.hasNext()) {
            return false;
          }
        }
      }
    }

    private static byte[] nullBytes = new byte[0];
    private static <T> T decode(byte[] bytes, Coder<T> coder) throws IOException {
      // If 'bytes' is null, use byte[0]. It is common for key in Kakfa record to be null.
      // This makes it impossible for user to distinguish between zero length byte and null.
      // Alternately, we could have a ByteArrayCoder that handles nulls, and use that for default
      // coder.
      byte[] toDecode = bytes == null ? nullBytes : bytes;
      return coder.decode(new ExposedByteArrayInputStream(toDecode), Coder.Context.OUTER);
    }

    // called from offsetFetcher thread
    private void updateLatestOffsets() {
      for (PartitionState p : partitionStates) {
        try {
          offsetConsumer.seekToEnd(p.topicPartition);
          long offset = offsetConsumer.position(p.topicPartition);
          p.setLatestOffset(offset);;
        } catch (Exception e) {
          LOG.warn(this + " : exception while fetching latest offsets. ignored.",  e);
          p.setLatestOffset(-1L); // reset
        }

        LOG.debug("{} : latest offset update for {} : {} (consumed offset {}, avg record size {})",
            this, p.topicPartition, p.latestOffset, p.consumedOffset, p.avgRecordSize);
      }
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
    public UnboundedSource<T, ?> getCurrentSource() {
      return source;
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      // should we delay updating consumed offset till this point? Mostly not required.
      return source.converterFn.apply(curRecord);
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return curTimestamp;
    }


    @Override
    public long getSplitBacklogBytes() {
      long backlogBytes = 0;
      for (PartitionState p : partitionStates) {
        long pBacklog = p.approxBacklogInBytes();
        if (pBacklog == UnboundedReader.BACKLOG_UNKNOWN) {
          return UnboundedReader.BACKLOG_UNKNOWN;
        }
        backlogBytes += pBacklog;
      }

      LOG.info("{} backlog reported : {}", this, backlogBytes);
      return backlogBytes;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      availableRecordsQueue.poll(); // drain unread batch, this unblocks consumer thread.
      consumer.wakeup();
      consumerPollThread.shutdown();
      offsetFetcherThread.shutdown();
      Closeables.close(offsetConsumer, true);
      Closeables.close(consumer, true);
    }
  }
}

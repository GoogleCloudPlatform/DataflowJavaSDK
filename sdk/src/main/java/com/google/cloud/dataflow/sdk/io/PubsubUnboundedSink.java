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

package com.google.cloud.dataflow.sdk.io;

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.NullableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData.Builder;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterFirst;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.PubsubClient;
import com.google.cloud.dataflow.sdk.util.PubsubClient.OutgoingMessage;
import com.google.cloud.dataflow.sdk.util.PubsubClient.PubsubClientFactory;
import com.google.cloud.dataflow.sdk.util.PubsubClient.TopicPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hashing;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import javax.annotation.Nullable;

/**
 * A PTransform which streams messages to Pubsub.
 * <ul>
 * <li>The underlying implementation is just a {@link GroupByKey} followed by a {@link ParDo} which
 * publishes as a side effect. (In the future we want to design and switch to a custom
 * {@code UnboundedSink} implementation so as to gain access to system watermark and
 * end-of-pipeline cleanup.)
 * <li>We try to send messages in batches while also limiting send latency.
 * <li>No stats are logged. Rather some counters are used to keep track of elements and batches.
 * <li>Though some background threads are used by the underlying netty system all actual Pubsub
 * calls are blocking. We rely on the underlying runner to allow multiple {@link DoFn} instances
 * to execute concurrently and hide latency.
 * <li>A failed bundle will cause messages to be resent. Thus we rely on the Pubsub consumer
 * to dedup messages.
 * </ul>
 *
 * <p>NOTE: This is not the implementation used when running on the Google Cloud Dataflow service.
 */
public class PubsubUnboundedSink<T> extends PTransform<PCollection<T>, PDone> {
  private static final Logger LOG = LoggerFactory.getLogger(PubsubUnboundedSink.class);

  /**
   * Default maximum number of messages per publish.
   */
  private static final int DEFAULT_PUBLISH_BATCH_SIZE = 1000;

  /**
   * Default maximum size of a publish batch, in bytes.
   */
  private static final int DEFAULT_PUBLISH_BATCH_BYTES = 400000;

  /**
   * Default longest delay between receiving a message and pushing it to Pubsub.
   */
  private static final Duration DEFAULT_MAX_LATENCY = Duration.standardSeconds(2);

  /**
   * Coder for conveying outgoing messages between internal stages.
   */
  private static class OutgoingMessageCoder extends CustomCoder<OutgoingMessage> {
    private static final NullableCoder<String> RECORD_ID_CODER =
        NullableCoder.of(StringUtf8Coder.of());

    @Override
    public void encode(
        OutgoingMessage value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      ByteArrayCoder.of().encode(value.elementBytes, outStream, Context.NESTED);
      BigEndianLongCoder.of().encode(value.timestampMsSinceEpoch, outStream, Context.NESTED);
      RECORD_ID_CODER.encode(value.recordId, outStream, Context.NESTED);
    }

    @Override
    public OutgoingMessage decode(
        InputStream inStream, Context context) throws CoderException, IOException {
      byte[] elementBytes = ByteArrayCoder.of().decode(inStream, Context.NESTED);
      long timestampMsSinceEpoch = BigEndianLongCoder.of().decode(inStream, Context.NESTED);
      @Nullable String recordId = RECORD_ID_CODER.decode(inStream, Context.NESTED);
      return new OutgoingMessage(elementBytes, timestampMsSinceEpoch, recordId);
    }
  }

  @VisibleForTesting
  static final Coder<OutgoingMessage> CODER = new OutgoingMessageCoder();

  // ================================================================================
  // RecordIdMethod
  // ================================================================================

  /**
   * Specify how record ids are to be generated.
   */
  @VisibleForTesting
  enum RecordIdMethod {
    /** Leave null. */
    NONE,
    /** Generate randomly. */
    RANDOM,
    /** Generate deterministically. For testing only. */
    DETERMINISTIC
  }

  // ================================================================================
  // ShardFn
  // ================================================================================

  /**
   * Convert elements to messages and shard them.
   */
  private static class ShardFn<T> extends DoFn<T, KV<Integer, OutgoingMessage>> {
    private final Aggregator<Long, Long> elementCounter =
        createAggregator("elements", new Sum.SumLongFn());
    private final Coder<T> elementCoder;
    private final int numShards;
    private final RecordIdMethod recordIdMethod;

    ShardFn(Coder<T> elementCoder, int numShards, RecordIdMethod recordIdMethod) {
      this.elementCoder = elementCoder;
      this.numShards = numShards;
      this.recordIdMethod = recordIdMethod;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      elementCounter.addValue(1L);
      byte[] elementBytes = CoderUtils.encodeToByteArray(elementCoder, c.element());
      long timestampMsSinceEpoch = c.timestamp().getMillis();
      @Nullable String recordId = null;
      switch (recordIdMethod) {
        case NONE:
          break;
        case DETERMINISTIC:
          recordId = Hashing.murmur3_128().hashBytes(elementBytes).toString();
          break;
        case RANDOM:
          // Since these elements go through a GroupByKey, any  failures while sending to
          // Pubsub will be retried without falling back and generating a new record id.
          // Thus even though we may send the same message to Pubsub twice, it is guaranteed
          // to have the same record id.
          recordId = UUID.randomUUID().toString();
          break;
      }
      c.output(KV.of(ThreadLocalRandom.current().nextInt(numShards),
                     new OutgoingMessage(elementBytes, timestampMsSinceEpoch, recordId)));
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("numShards", numShards));
    }
  }

  // ================================================================================
  // WriterFn
  // ================================================================================

  /**
   * Publish messages to Pubsub in batches.
   */
  private static class WriterFn
      extends DoFn<KV<Integer, Iterable<OutgoingMessage>>, Void> {
    private final PubsubClientFactory pubsubFactory;
    private final TopicPath topic;
    private final String timestampLabel;
    private final String idLabel;
    private final int publishBatchSize;
    private final int publishBatchBytes;

    /**
     * Client on which to talk to Pubsub. Null until created by {@link #startBundle}.
     */
    @Nullable
    private transient PubsubClient pubsubClient;

    private final Aggregator<Long, Long> batchCounter =
        createAggregator("batches", new Sum.SumLongFn());
    private final Aggregator<Long, Long> elementCounter =
        createAggregator("elements", new Sum.SumLongFn());
    private final Aggregator<Long, Long> byteCounter =
        createAggregator("bytes", new Sum.SumLongFn());

    WriterFn(
        PubsubClientFactory pubsubFactory, TopicPath topic, String timestampLabel,
        String idLabel, int publishBatchSize, int publishBatchBytes) {
      this.pubsubFactory = pubsubFactory;
      this.topic = topic;
      this.timestampLabel = timestampLabel;
      this.idLabel = idLabel;
      this.publishBatchSize = publishBatchSize;
      this.publishBatchBytes = publishBatchBytes;
    }

    /**
     * BLOCKING
     * Send {@code messages} as a batch to Pubsub.
     */
    private void publishBatch(List<OutgoingMessage> messages, int bytes)
        throws IOException {
      int n = pubsubClient.publish(topic, messages);
      checkState(n == messages.size(), "Attempted to publish %s messages but %s were successful",
                 messages.size(), n);
      batchCounter.addValue(1L);
      elementCounter.addValue((long) messages.size());
      byteCounter.addValue((long) bytes);
    }

    @Override
    public void startBundle(Context c) throws Exception {
      checkState(pubsubClient == null, "startBundle invoked without prior finishBundle");
      pubsubClient =
          pubsubFactory.newClient(timestampLabel, idLabel,
                                  c.getPipelineOptions().as(DataflowPipelineOptions.class));
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      List<OutgoingMessage> pubsubMessages = new ArrayList<>(publishBatchSize);
      int bytes = 0;
      for (OutgoingMessage message : c.element().getValue()) {
        if (!pubsubMessages.isEmpty()
            && bytes + message.elementBytes.length > publishBatchBytes) {
          // Break large (in bytes) batches into smaller.
          // (We've already broken by batch size using the trigger below, though that may
          // run slightly over the actual PUBLISH_BATCH_SIZE. We'll consider that ok since
          // the hard limit from Pubsub is by bytes rather than number of messages.)
          // BLOCKS until published.
          publishBatch(pubsubMessages, bytes);
          pubsubMessages.clear();
          bytes = 0;
        }
        pubsubMessages.add(message);
        bytes += message.elementBytes.length;
      }
      if (!pubsubMessages.isEmpty()) {
        // BLOCKS until published.
        publishBatch(pubsubMessages, bytes);
      }
    }

    @Override
    public void finishBundle(Context c) throws Exception {
      pubsubClient.close();
      pubsubClient = null;
    }

    @Override
    public void populateDisplayData(Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("topic", topic.getPath()));
      builder.add(DisplayData.item("transport", pubsubFactory.getKind()));
      builder.addIfNotNull(DisplayData.item("timestampLabel", timestampLabel));
      builder.addIfNotNull(DisplayData.item("idLabel", idLabel));
    }
  }

  // ================================================================================
  // PubsubUnboundedSink
  // ================================================================================

  /**
   * Which factory to use for creating Pubsub transport.
   */
  private final PubsubClientFactory pubsubFactory;

  /**
   * Pubsub topic to publish to.
   */
  private final TopicPath topic;

  /**
   * Coder for elements. It is the responsibility of the underlying Pubsub transport to
   * re-encode element bytes if necessary, eg as Base64 strings.
   */
  private final Coder<T> elementCoder;

  /**
   * Pubsub metadata field holding timestamp of each element, or {@literal null} if should use
   * Pubsub message publish timestamp instead.
   */
  @Nullable
  private final String timestampLabel;

  /**
   * Pubsub metadata field holding id for each element, or {@literal null} if need to generate
   * a unique id ourselves.
   */
  @Nullable
  private final String idLabel;

  /**
   * Number of 'shards' to use so that latency in Pubsub publish can be hidden. Generally this
   * should be a small multiple of the number of available cores. Too smoll a number results
   * in too much time lost to blocking Pubsub calls. To large a number results in too many
   * single-element batches being sent to Pubsub with high per-batch overhead.
   */
  private final int numShards;

  /**
   * Maximum number of messages per publish.
   */
  private final int publishBatchSize;

  /**
   * Maximum size of a publish batch, in bytes.
   */
  private final int publishBatchBytes;

  /**
   * Longest delay between receiving a message and pushing it to Pubsub.
   */
  private final Duration maxLatency;

  /**
   * How record ids should be generated for each record (if {@link #idLabel} is non-{@literal
   * null}).
   */
  private final RecordIdMethod recordIdMethod;

  @VisibleForTesting
  PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      TopicPath topic,
      Coder<T> elementCoder,
      String timestampLabel,
      String idLabel,
      int numShards,
      int publishBatchSize,
      int publishBatchBytes,
      Duration maxLatency,
      RecordIdMethod recordIdMethod) {
    this.pubsubFactory = pubsubFactory;
    this.topic = topic;
    this.elementCoder = elementCoder;
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    this.numShards = numShards;
    this.publishBatchSize = publishBatchSize;
    this.publishBatchBytes = publishBatchBytes;
    this.maxLatency = maxLatency;
    this.recordIdMethod = idLabel == null ? RecordIdMethod.NONE : recordIdMethod;
  }

  public PubsubUnboundedSink(
      PubsubClientFactory pubsubFactory,
      TopicPath topic,
      Coder<T> elementCoder,
      String timestampLabel,
      String idLabel,
      int numShards) {
    this(pubsubFactory, topic, elementCoder, timestampLabel, idLabel, numShards,
         DEFAULT_PUBLISH_BATCH_SIZE, DEFAULT_PUBLISH_BATCH_BYTES, DEFAULT_MAX_LATENCY,
         RecordIdMethod.RANDOM);
  }

  public TopicPath getTopic() {
    return topic;
  }

  @Nullable
  public String getTimestampLabel() {
    return timestampLabel;
  }

  @Nullable
  public String getIdLabel() {
    return idLabel;
  }

  public Coder<T> getElementCoder() {
    return elementCoder;
  }

  @Override
  public PDone apply(PCollection<T> input) {
    input.apply(
        Window.named("PubsubUnboundedSink.Window")
            .<T>into(new GlobalWindows())
            .triggering(
                Repeatedly.forever(
                    AfterFirst.of(AfterPane.elementCountAtLeast(publishBatchSize),
                                  AfterProcessingTime.pastFirstElementInPane()
                                                     .plusDelayOf(maxLatency))))
            .discardingFiredPanes())
         .apply(ParDo.named("PubsubUnboundedSink.Shard")
                     .of(new ShardFn<T>(elementCoder, numShards, recordIdMethod)))
         .setCoder(KvCoder.of(VarIntCoder.of(), CODER))
         .apply(GroupByKey.<Integer, OutgoingMessage>create())
         .apply(ParDo.named("PubsubUnboundedSink.Writer")
                     .of(new WriterFn(pubsubFactory, topic, timestampLabel, idLabel,
                                      publishBatchSize, publishBatchBytes)));
    return PDone.in(input.getPipeline());
  }
}

package com.google.cloud.dataflow.contrib.kafka.io;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.common.base.Throwables;
import com.workiva.cloud.dataflow.coders.ConsumerRecordCoder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;


/**
 * An {@code UnboundedSource} for reading a Kafka topic. Requires Kafka 0.9 or
 * later.
 *
 * <p>To read a {@link com.google.cloud.dataflow.sdk.values.PCollection} of
 * ConsumerRecords from one Kafka topic, use the
 * {@link KafkaSource#readFrom} method as a convenience
 * that returns a read transform. For example:
 *
 * <pre>
 * {@code
 * PCollection<ConsumerRecord<String, String>> records =
 * KafkaSource.readFrom(topic, bootstrapServers, groupId)
 * }
 * </pre>
 *
 * Limitations: Currently only supports String encoding of Consumer Records.
 */
public class KafkaSource extends UnboundedSource<ConsumerRecord<String, String>, KafkaCheckpoint> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private static final long serialVersionUID = 0L;

  private final String topic;
  private final String bootstrapServers;
  private final String groupId;
  private final boolean dedup;

  public static Read.Unbounded<ConsumerRecord<String, String>> readFrom(String topic,
      String bootstrapServers, String groupId) {
    return Read.from(new KafkaSource(topic, bootstrapServers, groupId, false));
  }

  /**
   * Enable deduplication of records.
   */
  public KafkaSource withDedup() {
    return new KafkaSource(topic, bootstrapServers, groupId, true);
  }

  private KafkaSource(String topic, String bootstrapServers, String groupId, boolean dedup) {
    this.topic = topic;
    this.bootstrapServers = bootstrapServers;
    this.groupId = groupId;
    this.dedup = dedup;
  }


  /**
   * Returns a list of {@code KafkaSource} objects representing the instances of this source
   * that should be used when executing the workflow.
   *
   * <p>Kafka automatically partitions the data among readers. In this case,
   * {@code n} identical replicas of the top-level source can be returned.
   */
  @Override
  public List<KafkaSource> generateInitialSplits(int desiredNumSplits, PipelineOptions options) {
    List<KafkaSource> splits = new ArrayList<>();
    for (int i = 0; i < desiredNumSplits; i++) {
      splits.add(new KafkaSource(topic, bootstrapServers, groupId, dedup));
    }
    return splits;
  }

  /**
   * KafkaCheckpoint does not need to be durably committed. Can return null.
   */
  @Override
  public Coder<KafkaCheckpoint> getCheckpointMarkCoder() {
    return null;
  }

  /**
   * Returns whether this source requires explicit deduping.
   *
   * <p>The KafkaCheckpoint will return duplicate records on error. If you
   * require deduping, use withDedup() when constructing the KafkaSource.
   */
  @Override
  public boolean requiresDeduping() {
    return dedup;
  }


  /**
   * A {@code Reader} that reads unbounded input from a single Kafka topic.
   */
  private class KafkaSourceReader extends UnboundedReader<ConsumerRecord<String, String>> {

    private final KafkaCheckpoint checkpoint;

    KafkaSourceReader(KafkaCheckpoint checkpoint,
        String bootstrapServers, String groupId, String topic) {
      if (checkpoint == null) {
        this.checkpoint = new KafkaCheckpoint(bootstrapServers, groupId, topic);
      } else {
        this.checkpoint = checkpoint;
      }
    }

    /**
     * Initializes the reader and advances the reader to the first record.
     */
    @Override
    public boolean start() {
      return this.advance();
    }

    /**
     * Advances the reader to the next valid record.
     *
     * <p>Returns {@code true} if a record was read, {@code false} if there is no more input
     * available. Future calls to {@link #advance} may return {@code true} once more data is
     * available.
     */
    @Override
    public boolean advance() {
      return this.checkpoint.advance();
    }

    /**
     * Returns the data record at the current position, last read by calling start or advance.
     */
    @Override
    public ConsumerRecord<String, String> getCurrent() throws NoSuchElementException {
      return this.checkpoint.current();
    }

    /**
     * Returns a unique identifier for the current record based
     * on the current offset, topic and partition.
     */
    @Override
    public byte[] getCurrentRecordId() throws NoSuchElementException {
      try {
        ConsumerRecord<String, String> record = getCurrent();
        if (record != null) {
          return CoderUtils.encodeToByteArray(StringUtf8Coder.of(), record.toString());
        } else {
          return new byte[0];
        }
      } catch (CoderException e) {
        LOG.error(Throwables.getStackTraceAsString(e));
        return new byte[0];
      }
    }

    /**
     * Returns a lower bound on timestamps of future elements read by this
     * reader.
     *
     * Kafka records do not have a natural timestamp so use clock time instead.
     */
    @Override
    public Instant getWatermark() {
      DateTime dt = new DateTime(DateTimeZone.UTC);
      return dt.toInstant();
    }

    /**
     * Returns the timestamp associated with the current data item.
     *
     * Kafka records do not have a natural timestamp so min value instead.
     */
    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      ConsumerRecord<String, String> record = getCurrent();
      if (record == null) {
        throw new NoSuchElementException();
      }
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    /**
     * Returns a {@link KafkaCheckpoint} representing the progress of this {@code UnboundedReader}.
     */
    @Override
    public CheckpointMark getCheckpointMark() {
      return checkpoint;
    }

    /**
     * Safely close the reader.
     */
    @Override
    public void close() {
      this.checkpoint.shutdown();
    }

    /**
     * Return an instance of the current KafkaSource.
     */
    @Override
    public KafkaSource getCurrentSource() {
      return KafkaSource.this;
    }
  }

  /**
   * Create a new UnboundedSource.UnboundedReader to read from this source,
   * resuming from the given checkpoint if present.
   */
  @Override
  public KafkaSourceReader createReader(PipelineOptions options,
      @Nullable KafkaCheckpoint checkpoint) {

    return new KafkaSourceReader(checkpoint, this.bootstrapServers, this.groupId, this.topic);
  }

  /**
   * Checks that this source is valid, before it can be used in a pipeline.
   */
  @Override
  public void validate() {}

  /**
   * Returns the default Coder to use for the data read from this source.
   */
  @Override
  public Coder<ConsumerRecord<String, String>> getDefaultOutputCoder() {
    return ConsumerRecordCoder.of();
  }
}

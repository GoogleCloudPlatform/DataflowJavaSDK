package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues
        .newArrayDeque;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.client.response.GetKinesisRecordsResult;
import com.google.cloud.dataflow.sdk.io.kinesis.client.response.KinesisRecord;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.CustomOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Deque;

/***
 * Iterates over records in a single shard.
 * Under the hood records are retrieved from Kinesis in batches and stored in the in-memory queue.
 * Then the caller of {@link ShardRecordsIterator#next()} can read from queue one by one.
 */
public class ShardRecordsIterator {
    private static final Logger LOG = LoggerFactory.getLogger(ShardRecordsIterator.class);

    private final SimplifiedKinesisClient kinesis;
    private final RecordFilter filter;
    private ShardCheckpoint checkpoint;
    private String shardIterator;
    private Deque<KinesisRecord> data = newArrayDeque();

    public ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
                                SimplifiedKinesisClient simplifiedKinesisClient) throws
            IOException {
        this(initialCheckpoint, simplifiedKinesisClient, new RecordFilter());
    }

    public ShardRecordsIterator(final ShardCheckpoint initialCheckpoint,
                                SimplifiedKinesisClient simplifiedKinesisClient,
                                RecordFilter filter) throws
            IOException {
        checkNotNull(initialCheckpoint);
        checkNotNull(simplifiedKinesisClient);

        this.checkpoint = initialCheckpoint;
        this.filter = filter;
        this.kinesis = simplifiedKinesisClient;
        shardIterator = checkpoint.getShardIterator(kinesis);
    }

    /***
     * Returns record if there's any present.
     * Returns absent() if there are no new records at this time in the shard.
     */
    public Optional<KinesisRecord> next() throws IOException {
        readMoreIfNecessary();

        if (data.isEmpty()) {
            return CustomOptional.absent();
        } else {
            KinesisRecord record = data.removeFirst();
            checkpoint = checkpoint.moveAfter(record);
            return CustomOptional.of(record);
        }
    }

    private void readMoreIfNecessary() throws IOException {
        if (data.isEmpty()) {
            GetKinesisRecordsResult response;
            try {
                response = kinesis.getRecords(shardIterator);
            } catch (ExpiredIteratorException e) {
                LOG.info("Refreshing expired iterator", e);
                shardIterator = checkpoint.getShardIterator(kinesis);
                response = kinesis.getRecords(shardIterator);
            }
            LOG.debug("Fetched {} new records", response.getRecords().size());
            shardIterator = response.getNextShardIterator();
            data.addAll(filter.apply(response.getRecords(), checkpoint));
        }
    }

    public ShardCheckpoint getCheckpoint() {
        return checkpoint;
    }


}

package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Iterables
        .transform;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Function;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.Shard;
import java.io.IOException;

/**
 * Creates {@link KinesisReaderCheckpoint}, which spans over all shards in given stream.
 * List of shards is obtained dynamically on call to {@link #generate(SimplifiedKinesisClient)}.
 */
public class DynamicCheckpointGenerator implements CheckpointGenerator {
    private final String streamName;
    private final InitialPositionInStream startPosition;

    public DynamicCheckpointGenerator(String streamName, InitialPositionInStream startPosition) {
        checkNotNull(streamName);
        checkNotNull(startPosition);

        this.streamName = streamName;
        this.startPosition = startPosition;
    }

    @Override
    public KinesisReaderCheckpoint generate(SimplifiedKinesisClient kinesis) throws IOException {
        return new KinesisReaderCheckpoint(
                transform(kinesis.listShards(streamName), new Function<Shard, ShardCheckpoint>() {
                    @Override
                    public ShardCheckpoint apply(Shard shard) {
                        return new ShardCheckpoint(streamName, shard.getShardId(), startPosition);
                    }
                })
        );
    }

    @Override
    public String toString() {
        return String.format("Checkpoint generator for %s: %s", streamName, startPosition);
    }
}

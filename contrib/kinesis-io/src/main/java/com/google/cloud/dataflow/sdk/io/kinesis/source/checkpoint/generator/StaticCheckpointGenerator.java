package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;

/**
 * Always returns the same instance of checkpoint.
 */
public class StaticCheckpointGenerator implements CheckpointGenerator {
    private final KinesisReaderCheckpoint checkpoint;

    public StaticCheckpointGenerator(KinesisReaderCheckpoint checkpoint) {
        checkNotNull(checkpoint);
        this.checkpoint = checkpoint;
    }

    @Override
    public KinesisReaderCheckpoint generate(SimplifiedKinesisClient client) {
        return checkpoint;
    }

    @Override
    public String toString() {
        return checkpoint.toString();
    }
}

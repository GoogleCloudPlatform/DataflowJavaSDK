package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator;

import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;

import java.io.IOException;
import java.io.Serializable;

/**
 * Used to generate checkpoint object on demand.
 * How exactly the checkpoint is generated is up to implementing class.
 */
public interface CheckpointGenerator extends Serializable {
    KinesisReaderCheckpoint generate(SimplifiedKinesisClient client) throws IOException;
}

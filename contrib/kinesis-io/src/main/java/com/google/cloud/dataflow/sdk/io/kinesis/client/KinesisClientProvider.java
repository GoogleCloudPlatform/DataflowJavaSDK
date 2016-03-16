package com.google.cloud.dataflow.sdk.io.kinesis.client;

import com.amazonaws.services.kinesis.AmazonKinesis;
import java.io.Serializable;

/**
 * Provides instances of {@link AmazonKinesis} interface.
 *
 * Please note, that any instance of {@link KinesisClientProvider} must be
 * {@link Serializable} to ensure it can be sent to worker machines.
 */
public interface KinesisClientProvider extends Serializable {
    AmazonKinesis get();
}

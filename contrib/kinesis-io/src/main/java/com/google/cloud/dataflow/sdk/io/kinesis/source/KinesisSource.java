package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.api.client.util.Lists.newArrayList;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Preconditions
        .checkNotNull;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.kinesis.client.KinesisClientProvider;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator.CheckpointGenerator;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator
        .DynamicCheckpointGenerator;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator
        .StaticCheckpointGenerator;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;


/***
 *
 */
public class KinesisSource extends UnboundedSource<byte[], KinesisReaderCheckpoint> {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSource.class);

    private final KinesisClientProvider kinesis;
    private CheckpointGenerator initialCheckpointGenerator;

    public KinesisSource(KinesisClientProvider kinesis, String streamName,
                         InitialPositionInStream initialPositionInStream) {
        this(kinesis, new DynamicCheckpointGenerator(streamName, initialPositionInStream));
    }

    KinesisSource(KinesisClientProvider kinesisClientProvider,
                  CheckpointGenerator initialCheckpoint) {
        this.kinesis = kinesisClientProvider;
        this.initialCheckpointGenerator = initialCheckpoint;
        validate();
    }

    @Override
    public List<KinesisSource> generateInitialSplits(int desiredNumSplits,
                                                     PipelineOptions options) throws Exception {
        KinesisReaderCheckpoint checkpoint =
                initialCheckpointGenerator.generate(SimplifiedKinesisClient.from(kinesis));

        List<KinesisSource> sources = newArrayList();

        for (KinesisReaderCheckpoint partition : checkpoint.splitInto(desiredNumSplits)) {
            sources.add(new KinesisSource(
                    kinesis,
                    new StaticCheckpointGenerator(partition)));
        }
        return sources;
    }

    @Override
    public UnboundedReader<byte[]> createReader(PipelineOptions options,
                                                KinesisReaderCheckpoint checkpointMark) {

        CheckpointGenerator checkpointGenerator = initialCheckpointGenerator;

        if (checkpointMark != null) {
            checkpointGenerator = new StaticCheckpointGenerator(checkpointMark);
        }

        LOG.info("Creating new reader using {}", checkpointGenerator);

        return new KinesisReader(
                SimplifiedKinesisClient.from(kinesis),
                checkpointGenerator,
                this);
    }

    @Override
    public Coder<KinesisReaderCheckpoint> getCheckpointMarkCoder() {
        return SerializableCoder.of(KinesisReaderCheckpoint.class);
    }

    @Override
    public void validate() {
        checkNotNull(kinesis);
        checkNotNull(initialCheckpointGenerator);
    }

    @Override
    public Coder<byte[]> getDefaultOutputCoder() {
        return ByteArrayCoder.of();
    }


}

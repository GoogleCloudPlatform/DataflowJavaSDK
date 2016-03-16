package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint;

import static com.google.api.client.util.Lists.newArrayList;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Iterables
        .transform;
import static com.google.common.collect.Lists.partition;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.kinesis.source.ShardRecordsIterator;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Function;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

/***
 * Checkpoint representing a total progress in a set of shards in single stream.
 * The set of shards covered by {@link KinesisReaderCheckpoint} may or may not be equal to set of
 * all shards present in the stream.
 * This class is immutable.
 */
public class KinesisReaderCheckpoint implements Iterable<ShardCheckpoint>, UnboundedSource
        .CheckpointMark, Serializable {
    private final List<ShardCheckpoint> shardCheckpoints;

    public KinesisReaderCheckpoint(Iterable<ShardCheckpoint> shardCheckpoints) {
        this.shardCheckpoints = ImmutableList.copyOf(shardCheckpoints);
    }

    public static KinesisReaderCheckpoint asCurrentStateOf(Iterable<ShardRecordsIterator>
                                                                   iterators) {
        return new KinesisReaderCheckpoint(transform(iterators,
                new Function<ShardRecordsIterator, ShardCheckpoint>() {

                    @Nullable
                    @Override
                    public ShardCheckpoint apply(@Nullable
                                                 ShardRecordsIterator shardRecordsIterator) {
                        assert shardRecordsIterator != null;
                        return shardRecordsIterator.getCheckpoint();
                    }
                }));
    }

    /***
     * Splits given multi-shard checkpoint into partitions of approximately equal size.
     *
     * @param desiredNumSplits - upper limit for number of partitions to generate.
     * @return list of checkpoints covering consecutive partitions of current checkpoint.
     */
    public List<KinesisReaderCheckpoint> splitInto(int desiredNumSplits) {
        int partitionSize = divideAndRoundUp(shardCheckpoints.size(), desiredNumSplits);

        List<KinesisReaderCheckpoint> checkpoints = newArrayList();
        for (List<ShardCheckpoint> shardPartition : partition(shardCheckpoints, partitionSize)) {
            checkpoints.add(new KinesisReaderCheckpoint(shardPartition));
        }
        return checkpoints;
    }

    private int divideAndRoundUp(int nominator, int denominator) {
        return (nominator + denominator - 1) / denominator;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {

    }

    @Override
    public Iterator<ShardCheckpoint> iterator() {
        return shardCheckpoints.iterator();
    }
}

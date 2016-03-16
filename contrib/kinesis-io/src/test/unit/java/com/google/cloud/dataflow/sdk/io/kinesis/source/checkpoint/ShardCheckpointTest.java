package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
        .LATEST;
import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
        .TRIM_HORIZON;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.kinesis.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import static org.fest.assertions.Assertions.assertThat;
import org.junit.Test;

/**
 * Created by ppastuszka on 14.03.16.
 */
public class ShardCheckpointTest {

    @Test
    public void testComparisonWithExtendedSequenceNumber() {
        assertThat(new ShardCheckpoint("", "", LATEST).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(new ShardCheckpoint("", "", TRIM_HORIZON).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "10", 1L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", 0L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isTrue();

        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", 0L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isFalse();

        assertThat(checkpoint(AT_SEQUENCE_NUMBER, "100", 1L).isBeforeOrAt(
                new ExtendedSequenceNumber("100", 0L)
        )).isFalse();

        assertThat(checkpoint(AFTER_SEQUENCE_NUMBER, "100", 0L).isBeforeOrAt(
                new ExtendedSequenceNumber("99", 1L)
        )).isFalse();
    }

    private ShardCheckpoint checkpoint(ShardIteratorType iteratorType, String sequenceNumber,
                                       long subSequenceNumber) {
        return new ShardCheckpoint("", "", iteratorType, sequenceNumber, subSequenceNumber);
    }
}

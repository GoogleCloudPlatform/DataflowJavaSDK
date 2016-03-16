package com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Iterables;

import static org.fest.assertions.Assertions.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static java.util.Arrays.asList;
import java.util.Iterator;
import java.util.List;

/***
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisReaderCheckpointTest {
    @Mock
    private ShardCheckpoint a, b, c;

    private KinesisReaderCheckpoint checkpoint;

    @Before
    public void setUp() {
        checkpoint = new KinesisReaderCheckpoint(asList(a, b, c));
    }

    @Test
    public void splitsCheckpointAccordingly() {
        verifySplitInto(1);
        verifySplitInto(2);
        verifySplitInto(3);
        verifySplitInto(4);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void isImmutable() {
        Iterator<ShardCheckpoint> iterator = checkpoint.iterator();
        iterator.remove();
    }

    private void verifySplitInto(int size) {
        List<KinesisReaderCheckpoint> split = checkpoint.splitInto(size);
        assertThat(Iterables.concat(split)).containsOnly(a, b, c);
        assertThat(split).hasSize(Math.min(size, 3));
    }
}

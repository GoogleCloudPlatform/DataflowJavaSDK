package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets.UTF_8;
import com.google.cloud.dataflow.sdk.io.kinesis.client.SimplifiedKinesisClient;
import com.google.cloud.dataflow.sdk.io.kinesis.client.response.KinesisRecord;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.KinesisReaderCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.generator.CheckpointGenerator;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.CustomOptional;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Optional;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import static java.util.Arrays.asList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * Created by ppastuszka on 12.12.15.
 */
@RunWith(MockitoJUnitRunner.class)
public class KinesisReaderTest {
    @Mock
    private SimplifiedKinesisClient kinesis;
    @Mock
    private CheckpointGenerator generator;
    @Mock
    private ShardCheckpoint firstCheckpoint, secondCheckpoint;
    @Mock
    private ShardRecordsIterator firstIterator, secondIterator;
    @Mock
    private KinesisRecord a, b, c, d;

    private KinesisReader reader;

    @Before
    public void setUp() throws IOException {
        when(generator.generate(kinesis)).thenReturn(new KinesisReaderCheckpoint(
                asList(firstCheckpoint, secondCheckpoint)
        ));
        when(firstCheckpoint.getShardRecordsIterator(kinesis)).thenReturn(firstIterator);
        when(secondCheckpoint.getShardRecordsIterator(kinesis)).thenReturn(secondIterator);
        when(firstIterator.next()).thenReturn(CustomOptional.<KinesisRecord>absent());
        when(secondIterator.next()).thenReturn(CustomOptional.<KinesisRecord>absent());

        when(a.getData()).thenReturn(asBytes("a"));
        when(b.getData()).thenReturn(asBytes("b"));
        when(c.getData()).thenReturn(asBytes("c"));
        when(d.getData()).thenReturn(asBytes("d"));

        reader = new KinesisReader(kinesis, generator, null);
    }

    @Test
    public void startReturnsFalseIfNoDataAtTheBeginning() throws IOException {
        assertThat(reader.start()).isFalse();
    }

    @Test(expected = NoSuchElementException.class)
    public void throwsNoSuchElementExceptionIfNoData() throws IOException {
        reader.start();
        reader.getCurrent();
    }

    @Test
    public void startReturnsTrueIfSomeDataAvailable() throws IOException {
        when(firstIterator.next()).
                thenReturn(Optional.of(a)).
                thenReturn(CustomOptional.<KinesisRecord>absent());

        assertThat(reader.start()).isTrue();
    }

    @Test
    public void readsThroughAllDataAvailable() throws IOException {
        when(firstIterator.next()).
                thenReturn(CustomOptional.<KinesisRecord>absent()).
                thenReturn(Optional.of(a)).
                thenReturn(CustomOptional.<KinesisRecord>absent()).
                thenReturn(Optional.of(b)).
                thenReturn(CustomOptional.<KinesisRecord>absent());

        when(secondIterator.next()).
                thenReturn(Optional.of(c)).
                thenReturn(CustomOptional.<KinesisRecord>absent()).
                thenReturn(Optional.of(d)).
                thenReturn(CustomOptional.<KinesisRecord>absent());

        assertThat(reader.start()).isTrue();
        assertThat(fromBytes(reader.getCurrent())).isEqualTo("c");
        assertThat(reader.advance()).isTrue();
        assertThat(fromBytes(reader.getCurrent())).isEqualTo("a");
        assertThat(reader.advance()).isTrue();
        assertThat(fromBytes(reader.getCurrent())).isEqualTo("d");
        assertThat(reader.advance()).isTrue();
        assertThat(fromBytes(reader.getCurrent())).isEqualTo("b");
        assertThat(reader.advance()).isFalse();
    }

    private String fromBytes(byte[] bytes) {
        return new String(bytes, UTF_8);
    }

    private ByteBuffer asBytes(String text) {
        return ByteBuffer.wrap(text.getBytes(UTF_8));
    }
}

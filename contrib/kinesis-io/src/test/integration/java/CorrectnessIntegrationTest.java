import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.KinesisIO;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import static org.fest.assertions.Assertions.assertThat;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import static utils.TestUtils.getTestKinesisClientProvider;
import utils.TestConfiguration;
import utils.TestUtils;

/***
 *
 */
public class CorrectnessIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(CorrectnessIntegrationTest.class);
    private static final long PIPELINE_STARTUP_TIME = TimeUnit.SECONDS.toMillis(10);
    private static final long ADDITIONAL_PROCESSING_TIME = TimeUnit.SECONDS.toMillis(20);
    private static final long RECORD_GENERATION_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    public static final long TOTAL_PROCESSING_TIME = PIPELINE_STARTUP_TIME +
            RECORD_GENERATION_TIMEOUT +
            ADDITIONAL_PROCESSING_TIME;
    private ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    private List<String> testData;

    @Before
    public void setUp() {
        testData = TestUtils.randomStrings(50000);
    }

    @Test
    public void readerTestWithKinesisProducer() throws Exception {
        Future<?> future = startTestPipeline();
        TestUtils.putRecordsWithKinesisProducer(testData, RECORD_GENERATION_TIMEOUT);
        LOG.info("All data sent to kinesis");
        future.get();
    }

    @Test
    public void readerTestWithOldStylePuts() throws Exception {
        Future<?> future = startTestPipeline();
        TestUtils.putRecordsOldStyle(testData, RECORD_GENERATION_TIMEOUT);
        LOG.info("All data sent to kinesis");
        future.get();
    }

    private Future<?> startTestPipeline() throws InterruptedException {
        final Pipeline p = TestPipeline.create();
        ((DirectPipelineRunner) p.getRunner()).getPipelineOptions().setStreaming(true);
        ((StreamingOptions) p.getOptions()).setStreaming(true);
        PCollection<String> result = p.
                apply(KinesisIO.Read.
                        from(
                                TestConfiguration.get().getTestKinesisStream(),
                                InitialPositionInStream.LATEST).
                        using(getTestKinesisClientProvider()).
                        withMaxReadTime(Duration.millis(
                                TOTAL_PROCESSING_TIME)
                        )
                ).
                apply(ParDo.of(new TestUtils.ByteArrayToString()));
        DataflowAssert.that(result).containsInAnyOrder(testData);

        Future<?> future = singleThreadExecutor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                PipelineResult result = p.run();
                PipelineResult.State state = result.getState();
                while (state != PipelineResult.State.DONE && state != PipelineResult.State.FAILED) {
                    Thread.sleep(1000);
                    state = result.getState();
                }
                assertThat(state).isEqualTo(PipelineResult.State.DONE);
                return null;
            }
        });
        Thread.sleep(PIPELINE_STARTUP_TIME);
        return future;
    }
}

import static com.google.api.client.repackaged.com.google.common.base.Strings.commonPrefix;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Sets.newHashSet;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.compute.model.Instance;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Sets;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.common.util.concurrent.ListenableFuture;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import static org.fest.assertions.Assertions.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.lang.System.currentTimeMillis;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import utils.BQ;
import utils.GCE;
import utils.PubSubUtil;
import utils.TestConfiguration;
import utils.TestUtils;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class CorrectnessE2ETest {
    private static final Logger LOG = LoggerFactory.getLogger(CorrectnessE2ETest.class);

    private TableReference testTable;
    private DataflowPipelineJob job;

    @Before
    public void setUp() throws IOException {
        job = null;
        testTable = TestUtils.getTestTableReference();
        BQ.get().deleteTableIfExists(testTable);
        BQ.get().createTable(testTable, TestUtils.getTestTableSchema());
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        BQ.get().deleteTableIfExists(testTable);
        if (job != null) {
            job.cancel();
            while (job.getState() != PipelineResult.State.CANCELLED) {
                LOG.info("Waiting for job to finish. Current state is {}", job.getState());
                Thread.sleep(5000);
            }
        }
    }

    @Test
    public void testSimpleCorrectnessOnDataflowService() throws InterruptedException,
            IOException, TimeoutException {
        job = TestUtils.runKinesisToBigQueryJob(testTable);
        LOG.info("Sending events to kinesis");

        List<String> testData = TestUtils.randomStrings(20000);
        TestUtils.putRecordsWithKinesisProducer(testData);

        verifyDataPresentInBigQuery(testData, TimeUnit.MINUTES.toMillis(2));
    }

    @Test
    public void dealsWithInstanceBeingRestarted() throws InterruptedException, IOException,
            TimeoutException {
        job = TestUtils.runKinesisToBigQueryJob(testTable);
        LOG.info("Sending events to kinesis");

        List<String> testData = TestUtils.randomStrings(40000);
        List<ListenableFuture<UserRecordResult>> futures = TestUtils
                .startPuttingRecordsWithKinesisProducer(testData);
        Instance randomInstance = chooseRandomInstance();
        GCE.get().stopInstance(randomInstance);
        TestUtils.waitForRecordsToBeSentToKinesis(futures);

        List<String> newTestData = TestUtils.randomStrings(40000);
        futures = TestUtils.startPuttingRecordsWithKinesisProducer(newTestData);
        testData.addAll(newTestData);
        GCE.get().startInstance(randomInstance);
        TestUtils.waitForRecordsToBeSentToKinesis(futures);

        verifyDataPresentInBigQuery(testData, TimeUnit.MINUTES.toMillis(5));
    }

    @Test
    @Ignore
    public void dealsWithInstanceBeingRestartedOnPubSub() throws InterruptedException,
            IOException, ExecutionException {
        job = TestUtils.runPubSubToBigQueryJob(testTable);
        LOG.info("Sending events to kinesis");

        List<String> testData = TestUtils.randomStrings(40000);
        List<Future<?>> futures = PubSubUtil.get()
                .startSendingRecordsToPubSub(testData);
        Instance randomInstance = chooseRandomInstance();
        GCE.get().stopInstance(randomInstance);
        PubSubUtil.get().waitForRecordsToBeSentToPubSub(futures);

        List<String> newTestData = TestUtils.randomStrings(40000);
        futures = PubSubUtil.get().startSendingRecordsToPubSub(newTestData);
        testData.addAll(newTestData);
        GCE.get().startInstance(randomInstance);
        PubSubUtil.get().waitForRecordsToBeSentToPubSub(futures);

        verifyDataPresentInBigQuery(testData, TimeUnit.MINUTES.toMillis(5));
    }

    private Instance chooseRandomInstance() throws IOException {
        List<Instance> currentDataflowInstances = getCurrentDataflowInstances();
        int randomIndex = TestUtils.RANDOM.nextInt(currentDataflowInstances.size());
        return currentDataflowInstances.get(randomIndex);
    }

    private List<Instance> getCurrentDataflowInstances() throws IOException {
        List<Instance> allInstances = GCE.get()
                .listInstances(TestConfiguration.get().getTestProject());

        List<Instance> currentDataflowInstances = Lists.newArrayList();
        for (Instance instance : allInstances) {
            String prefix = commonPrefix(TestUtils.getJobName().toLowerCase(), instance.getName()
                    .toLowerCase());
            if (prefix.length() >= 20) {
                currentDataflowInstances.add(instance);
            }
        }
        return currentDataflowInstances;
    }


    private void verifyDataPresentInBigQuery(List<String> testData, long timeout) throws
            IOException,
            InterruptedException {
        LOG.info("Waiting for pipeline to process all sent data");

        long sleepPeriod = TimeUnit.SECONDS.toMillis(30);
        long startTime = currentTimeMillis();
        AssertionError lastException = null;
        while (currentTimeMillis() - startTime <= timeout) {
            try {
                verifySingleDataInBigQuery(testData);
                return;
            } catch (AssertionError e) {
                lastException = e;
                LOG.warn("Data in BigQuery not yet ready", e);
                Thread.sleep(sleepPeriod);
            }
        }
        throw lastException;
    }

    private void verifySingleDataInBigQuery(List<String> testData) throws IOException {
        LOG.info("Veryfing result in BigQuery");
        List<String> dataFromBQ = BQ.get().readAllFrom(testTable);
        HashSet<String> setOfExpectedData = newHashSet(testData);
        HashSet<String> setOfDataInBQ = newHashSet(dataFromBQ);

        Set<String> dataNotInBQ = Sets.difference(setOfExpectedData, setOfDataInBQ);
        Set<String> redundantDataInBQ = Sets.difference(setOfDataInBQ, setOfExpectedData);

        assertThat(dataNotInBQ).isEmpty();
        assertThat(redundantDataInBQ).isEmpty();
    }
}

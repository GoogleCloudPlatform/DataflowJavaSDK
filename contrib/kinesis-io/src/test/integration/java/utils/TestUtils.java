package utils;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkNotNull;
import static com.google.api.client.util.Lists.newArrayList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.KinesisIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;
import com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.util.concurrent.ListenableFuture;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordFailedException;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import static org.joda.time.Duration.standardDays;
import static org.joda.time.Duration.standardSeconds;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/***
 *
 */
public class TestUtils {

    public static final SecureRandom RANDOM = new SecureRandom();

    public static String randomString() {
        return new BigInteger(130, RANDOM).toString(32);
    }

    public static List<String> randomStrings(int howMany) {
        List<String> data = newArrayList();
        for (int i = 0; i < howMany; ++i) {
            data.add(TestUtils.randomString());
        }
        return data;
    }

    public static TableSchema getTestTableSchema() {
        return new TableSchema().
                setFields(asList(
                        new TableFieldSchema()
                                .setName("a")
                                .setType("STRING")));
    }

    public static TableReference getTestTableReference() {
        return new TableReference().
                setProjectId(TestConfiguration.get().getTestProject()).
                setDatasetId(TestConfiguration.get().getTestDataset()).
                setTableId(getTestTableId());
    }

    public static String getTestTableId() {
        return randomString();
    }

    public static AWSCredentialsProvider getTestAwsCredentialsProvider() {
        return getStaticCredentialsProvider(
                TestConfiguration.get().getAwsAccessKey(),
                TestConfiguration.get().getAwsSecretKey()
        );
    }

    private static AWSCredentialsProvider getStaticCredentialsProvider(String accessKey,
                                                                       String secretKey) {
        return new StaticCredentialsProvider(new BasicAWSCredentials(
                accessKey, secretKey
        ));
    }

    public static DataflowPipelineOptions getTestPipelineOptions() {
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        options.setProject(TestConfiguration.get().getTestProject());
        options.setStreaming(true);
        options.setJobName(getJobName());
        options.setRunner(DataflowPipelineRunner.class);
        options.setStagingLocation(TestConfiguration.get().getTestStagingLocation());
        options.setTempLocation(TestConfiguration.get().getTestTempLocation());
        return options;
    }

    public static String getJobName() {
        return "e2eKinesisConnectorCorrectness";
    }

    public static DataflowPipelineJob runKinesisToBigQueryJob(TableReference targetTable)
            throws InterruptedException {
        DataflowPipelineOptions options = getTestPipelineOptions();
        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.
                apply(KinesisIO.Read.
                        from(
                                TestConfiguration.get().getTestKinesisStream(),
                                InitialPositionInStream.LATEST).
                        using(getTestKinesisClientProvider())).
                apply(ParDo.of(new ByteArrayToString()));

        return runBqJob(targetTable, options, p, input);
    }

    public static DataflowPipelineJob runPubSubToBigQueryJob(TableReference targetTable)
            throws InterruptedException {
        DataflowPipelineOptions options = getTestPipelineOptions();
        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.apply(PubsubIO.Read.topic(TestConfiguration.get()
                .getTestPubSubTopic()));

        return runBqJob(targetTable, options, p, input);
    }

    private static DataflowPipelineJob runBqJob(TableReference targetTable,
                                                DataflowPipelineOptions options, Pipeline p,
                                                PCollection<String> input) throws
            InterruptedException {
        input.apply(Window.<String>into(FixedWindows.of(standardSeconds(10))).
                withAllowedLateness(standardDays(1))).
                apply(ParDo.of(new ToTableRow())).
                apply(BigQueryIO.Write.
                        to(targetTable).
                        withSchema(TestUtils.getTestTableSchema()));
        DataflowPipelineJob job = DataflowPipelineRunner.fromOptions(options).run(p);
        while (job.getState() != PipelineResult.State.RUNNING) {
            Thread.sleep(1000);
        }
        Thread.sleep(1000 * 60 * 3);
        return job;
    }

    public static void putRecordsWithKinesisProducer(List<String> data) throws TimeoutException {
        putRecordsWithKinesisProducer(data, Long.MAX_VALUE);
    }

    public static void putRecordsWithKinesisProducer(List<String> data, long timeout) throws
            TimeoutException {
        long startTime = currentTimeMillis();
        List<ListenableFuture<UserRecordResult>> futures = startPuttingRecordsWithKinesisProducer
                (data);

        waitForRecordsToBeSentToKinesis(futures, timeout - (currentTimeMillis() - startTime));
    }

    public static void waitForRecordsToBeSentToKinesis(List<ListenableFuture<UserRecordResult>>
                                                               futures) throws TimeoutException {
        waitForRecordsToBeSentToKinesis(futures, Long.MAX_VALUE);
    }

    public static void waitForRecordsToBeSentToKinesis(List<ListenableFuture<UserRecordResult>>
                                                               futures, long timeout) throws
            TimeoutException {
        long startTime = currentTimeMillis();

        for (ListenableFuture<UserRecordResult> future : futures) {
            try {
                long timeLeft = verifyTimeLeft(timeout, startTime);
                UserRecordResult result = future.get(timeLeft, TimeUnit.MILLISECONDS);
                verifyResult(result);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UserRecordFailedException) {
                    verifyResult(((UserRecordFailedException) e.getCause()).getResult());
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static long verifyTimeLeft(long timeout, long startTime) throws TimeoutException {
        long timeLeft = timeout - (currentTimeMillis() - startTime);
        if (timeLeft < 0) {
            throw new TimeoutException();
        }
        return timeLeft;
    }

    private static void verifyResult(UserRecordResult result) {
        if (!result.isSuccessful()) {
            throw new RuntimeException("Failed to send record: " + result.getAttempts().get(0)
                    .getErrorMessage());
        }
    }

    public static List<ListenableFuture<UserRecordResult>>
    startPuttingRecordsWithKinesisProducer(List<String> data) {
        KinesisProducer producer = new KinesisProducer(
                new KinesisProducerConfiguration().
                        setRateLimit(90).
                        setCredentialsProvider(getTestAwsCredentialsProvider()).
                        setRegion(TestConfiguration.get().getTestRegion())
        );
        List<ListenableFuture<UserRecordResult>> futures = newArrayList();
        for (String s : data) {
            ListenableFuture<UserRecordResult> future = producer.addUserRecord(
                    TestConfiguration.get().getTestKinesisStream(),
                    Integer.toString(s.hashCode()),
                    ByteBuffer.wrap(s.getBytes(Charsets.UTF_8)));
            futures.add(future);
        }
        return futures;
    }

    public static void putRecordsOldStyle(List<String> data, long timeout) throws TimeoutException {
        long startTime = currentTimeMillis();
        List<List<String>> partitions = Lists.partition(data, 499);

        AmazonKinesisClient client = new AmazonKinesisClient
                (getTestAwsCredentialsProvider())
                .withRegion(
                        Regions.fromName(TestConfiguration.get().getTestRegion()));
        for (List<String> partition : partitions) {
            List<PutRecordsRequestEntry> allRecords = newArrayList();
            for (String row : partition) {
                allRecords.add(new PutRecordsRequestEntry().
                        withData(ByteBuffer.wrap(row.getBytes(Charsets.UTF_8))).
                        withPartitionKey(Integer.toString(row.hashCode()))

                );
            }

            PutRecordsResult result;
            do {
                verifyTimeLeft(timeout, startTime);

                result = client.putRecords(
                        new PutRecordsRequest().
                                withStreamName(TestConfiguration.get().getTestKinesisStream()).
                                withRecords(allRecords));
                List<PutRecordsRequestEntry> failedRecords = newArrayList();
                int i = 0;
                for (PutRecordsResultEntry row : result.getRecords()) {
                    if (row.getErrorCode() != null) {
                        failedRecords.add(allRecords.get(i));
                    }
                    ++i;
                }
                allRecords = failedRecords;
            }

            while (result.getFailedRecordCount() > 0);
        }
    }

    public static TestKinesisClientProvider getTestKinesisClientProvider() {
        return new TestKinesisClientProvider();
    }

    /***
     *
     */
    public static class ToTableRow extends DoFn<String, TableRow> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            checkNotNull(c.element());
            c.output(new TableRow().set("a", c.element()));
        }
    }

    /***
     *
     */
    public static class ByteArrayToString extends DoFn<byte[], String> {
        @Override
        public void processElement(ProcessContext c) throws Exception {
            checkNotNull(c.element());
            c.output(new String(c.element(), Charsets.UTF_8));
        }
    }
}

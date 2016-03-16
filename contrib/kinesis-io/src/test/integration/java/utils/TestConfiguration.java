package utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class TestConfiguration {
    private final Map<String, String> configuration;

    private TestConfiguration() {
        InputStream rawConfig = getClass().getResourceAsStream("/testconfig.json");
        try {
            configuration = new ObjectMapper().readValue(rawConfig, HashMap.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                rawConfig.close();
            } catch (IOException e) {
                // do nothing
            }
        }
    }

    public static TestConfiguration get() {
        return Holder.INSTANCE;
    }

    public String getTestStagingLocation() {
        return constructTestBucketPath("staging");
    }

    public String getTestTempLocation() {
        return constructTestBucketPath("tmp");
    }

    private String constructTestBucketPath(String directory) {
        return "gs://" + Paths.get(getTestBucket(), "dataflow", directory).toString();
    }

    public String getTestBucket() {
        return configuration.get("DATAFLOW_TEST_BUCKET");
    }

    public String getTestProject() {
        return configuration.get("DATAFLOW_TEST_PROJECT");
    }

    public String getTestDataset() {
        return configuration.get("DATAFLOW_TEST_DATASET");
    }

    public String getTestKinesisStream() {
        return configuration.get("TEST_KINESIS_STREAM");
    }

    public String getAwsSecretKey() {
        return configuration.get("AWS_SECRET_KEY");
    }

    public String getAwsAccessKey() {
        return configuration.get("AWS_ACCESS_KEY");
    }

    public String getTestPubSubTopic() {
        return configuration.get("PUB_SUB_TEST_TOPIC");
    }

    public String getClusterAwsAccessKey() {
        return configuration.getOrDefault("REMOTE_AWS_ACCESS_KEY", getAwsAccessKey());
    }

    public String getClusterAwsSecretKey() {
        return configuration.getOrDefault("REMOTE_AWS_SECRET_KEY", getAwsSecretKey());
    }

    public String getClusterAwsRoleToAssume() {
        return configuration.get("REMOTE_AWS_ROLE_TO_ASSUME");
    }

    public String getTestRegion() {
        return configuration.get("TEST_KINESIS_STREAM_REGION");
    }

    private static class Holder {
        private static final TestConfiguration INSTANCE = new TestConfiguration();
    }
}

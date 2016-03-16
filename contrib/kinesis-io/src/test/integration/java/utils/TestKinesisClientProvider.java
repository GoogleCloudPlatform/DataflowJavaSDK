package utils;

import com.google.cloud.dataflow.sdk.io.kinesis.client.KinesisClientProvider;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;


/***
 *
 */
public class TestKinesisClientProvider implements KinesisClientProvider {
    private static final long serialVersionUID = 0L;

    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final String roleToAssume;

    public TestKinesisClientProvider() {
        accessKey = TestConfiguration.get().getClusterAwsAccessKey();
        secretKey = TestConfiguration.get().getClusterAwsSecretKey();
        region = TestConfiguration.get().getTestRegion();
        roleToAssume = TestConfiguration.get().getClusterAwsRoleToAssume();
    }

    private AWSCredentialsProvider getCredentialsProvider() {
        AWSCredentials credentials = new BasicAWSCredentials(
                accessKey,
                secretKey
        );

        if (roleToAssume != null) {
            return new
                    STSAssumeRoleSessionCredentialsProvider(
                    credentials, roleToAssume, "session"
            );
        }
        return new StaticCredentialsProvider(credentials);
    }

    @Override
    public AmazonKinesis get() {
        return new AmazonKinesisClient(getCredentialsProvider())
                .withRegion(Regions.fromName(region));
    }
}

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.io.kinesis.client.KinesisClientProvider;
import com.google.cloud.dataflow.sdk.io.kinesis.source.KinesisSource;
import com.google.cloud.dataflow.sdk.transforms.PTransform;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

/**
 * {@link PTransform}s for reading from
 * <a href="https://aws.amazon.com/kinesis/">Kinesis</a> streams.
 */
public class KinesisIO {
    /***
     * A {@link PTransform} that reads from a Kinesis stream.
     */
    public static class Read {

        private final String streamName;
        private final InitialPositionInStream initialPosition;

        private Read(String streamName, InitialPositionInStream initialPosition) {
            this.streamName = streamName;
            this.initialPosition = initialPosition;
        }

        /***
         * Specify reading from streamName at some initial position.
         */
        public static Read from(String streamName, InitialPositionInStream initialPosition) {
            return new Read(streamName, initialPosition);
        }

        /***
         * Allows to specify custom {@link KinesisClientProvider}.
         * {@link KinesisClientProvider} provides {@link AmazonKinesis} instances which are later
         * used for communication with Kinesis.
         * You should use this method if {@link Read#using(String, String, Regions)} does not
         * suite your needs.
         */
        public com.google.cloud.dataflow.sdk.io.Read.Unbounded<byte[]> using
                (KinesisClientProvider kinesisClientProvider) {
            return com.google.cloud.dataflow.sdk.io.Read.from(
                    new KinesisSource(kinesisClientProvider, streamName,
                            initialPosition));
        }

        /***
         * Specify credential details and region to be used to read from Kinesis.
         * If you need more sophisticated credential protocol, then you should look at
         * {@link Read#using(KinesisClientProvider)}.
         */
        public com.google.cloud.dataflow.sdk.io.Read.Unbounded<byte[]> using(String awsAccessKey,
                                                                             String awsSecretKey,
                                                                             Regions region) {
            return using(new BasicKinesisProvider(awsAccessKey, awsSecretKey, region));
        }

        private static class BasicKinesisProvider implements KinesisClientProvider {

            private final String accessKey;
            private final String secretKey;
            private final Regions region;

            private BasicKinesisProvider(String accessKey, String secretKey, Regions region) {
                this.accessKey = accessKey;
                this.secretKey = secretKey;
                this.region = region;
            }


            private AWSCredentialsProvider getCredentialsProvider() {
                return new StaticCredentialsProvider(new BasicAWSCredentials(
                        accessKey,
                        secretKey
                ));

            }

            @Override
            public AmazonKinesis get() {
                return new AmazonKinesisClient(getCredentialsProvider()).withRegion(region);
            }
        }
    }
}

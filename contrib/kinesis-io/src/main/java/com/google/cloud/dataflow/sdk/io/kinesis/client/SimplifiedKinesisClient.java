package com.google.cloud.dataflow.sdk.io.kinesis.client;

import com.google.cloud.dataflow.sdk.io.kinesis.client.response.GetKinesisRecordsResult;
import com.google.common.collect.Lists;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.ExpiredIteratorException;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.LimitExceededException;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.services.kinesis.model.StreamDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/***
 * Wraps {@link AmazonKinesis} class providing much simpler interface and
 * proper error handling.
 */
public class SimplifiedKinesisClient {
    private static final Logger LOG = LoggerFactory.getLogger(SimplifiedKinesisClient.class);

    private final AmazonKinesis kinesis;

    public SimplifiedKinesisClient(AmazonKinesis kinesis) {
        this.kinesis = kinesis;
    }

    public static SimplifiedKinesisClient from(KinesisClientProvider provider) {
        return new SimplifiedKinesisClient(provider.get());
    }

    public String getShardIterator(final String streamName, final String shardId,
                                   final ShardIteratorType shardIteratorType,
                                   final String startingSequenceNumber) throws IOException {
        return wrapExceptions(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return kinesis.getShardIterator(
                        streamName, shardId, shardIteratorType.toString(), startingSequenceNumber)
                        .getShardIterator();
            }
        });
    }

    public List<Shard> listShards(final String streamName) throws IOException {
        return wrapExceptions(new Callable<List<Shard>>() {
            @Override
            public List<Shard> call() throws Exception {
                List<Shard> shards = Lists.newArrayList();
                String lastShardId = null;

                StreamDescription description;
                do {
                    description = kinesis.describeStream(streamName, lastShardId)
                            .getStreamDescription();

                    shards.addAll(description.getShards());
                    lastShardId = shards.get(shards.size() - 1).getShardId();
                } while (description.getHasMoreShards());

                return shards;
            }
        });
    }

    /***
     * Gets records from Kinesis and deaggregates them if needed.
     *
     * @return list of deaggregated records
     * @throws IOException - in case of recoverable situation
     */
    public GetKinesisRecordsResult getRecords(String shardIterator) throws IOException {
        return getRecords(shardIterator, null);
    }

    /***
     * Gets records from Kinesis and deaggregates them if needed.
     *
     * @return list of deaggregated records
     * @throws IOException - in case of recoverable situation
     */
    public GetKinesisRecordsResult getRecords(final String shardIterator, final Integer limit)
            throws
            IOException {
        return wrapExceptions(new Callable<GetKinesisRecordsResult>() {
            @Override
            public GetKinesisRecordsResult call() throws Exception {
                GetRecordsResult response = kinesis.getRecords(new GetRecordsRequest()
                        .withShardIterator(shardIterator)
                        .withLimit(limit));
                return new GetKinesisRecordsResult(
                        UserRecord.deaggregate(response.getRecords()),
                        response.getNextShardIterator());
            }
        });
    }

    /***
     * Wraps Amazon specific exceptions into more friendly format.
     *
     * @throws IOException - in case of recoverable situation, i.e.
     *     the request rate is too high, Kinesis remote service failed, network issue, etc.
     * @throws ExpiredIteratorException - if iterator needs to be refreshed
     * @throws RuntimeException - in all other cases
     */
    private <T> T wrapExceptions(Callable<T> callable) throws IOException {
        try {
            return callable.call();
        } catch (ExpiredIteratorException e) {
            throw e;
        } catch (LimitExceededException | ProvisionedThroughputExceededException e) {
            LOG.warn("Too many requests to Kinesis", e);
            throw new IOException(e);
        } catch (AmazonServiceException e) {
            if (e.getErrorType() == AmazonServiceException.ErrorType.Service) {
                LOG.warn("Kinesis backend failed", e);
                throw new IOException(e);
            }
            LOG.error("Client side failure", e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.error("Unknown failure", e);
            throw new RuntimeException(e);
        }
    }

}

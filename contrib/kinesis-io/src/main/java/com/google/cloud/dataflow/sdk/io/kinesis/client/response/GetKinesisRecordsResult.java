package com.google.cloud.dataflow.sdk.io.kinesis.client.response;

import static com.google.common.collect.Lists.transform;
import com.google.common.base.Function;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import java.util.List;
import javax.annotation.Nullable;

/***
 *
 */
public class GetKinesisRecordsResult {
    private final List<KinesisRecord> records;
    private final String nextShardIterator;

    public GetKinesisRecordsResult(List<UserRecord> records, String nextShardIterator) {
        this.records = transform(records, new Function<UserRecord, KinesisRecord>() {
            @Nullable
            @Override
            public KinesisRecord apply(@Nullable UserRecord input) {
                assert input != null;  // to make FindBugs happy
                return new KinesisRecord(input);
            }
        });
        this.nextShardIterator = nextShardIterator;
    }

    public List<KinesisRecord> getRecords() {
        return records;
    }

    public String getNextShardIterator() {
        return nextShardIterator;
    }
}

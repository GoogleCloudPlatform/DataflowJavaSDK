package com.google.cloud.dataflow.sdk.io.kinesis.source;

import static com.google.api.client.util.Lists.newArrayList;
import com.google.cloud.dataflow.sdk.io.kinesis.client.response.KinesisRecord;
import com.google.cloud.dataflow.sdk.io.kinesis.source.checkpoint.ShardCheckpoint;

import java.util.List;

/**
 * Filters out records, which were already processed and checkpointed.
 *
 * We need this step, because we can get iterators from Kinesis only with "sequenceNumber" accuracy,
 * not with "subSequenceNumber" accuracy.
 */
class RecordFilter {
    public List<KinesisRecord> apply(List<KinesisRecord> records, ShardCheckpoint checkpoint) {
        List<KinesisRecord> filteredRecords = newArrayList();
        for (KinesisRecord record : records) {
            if (checkpoint.isBeforeOrAt(record.getExtendedSequenceNumber())) {
                filteredRecords.add(record);
            }
        }
        return filteredRecords;
    }
}

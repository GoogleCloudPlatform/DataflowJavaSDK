package com.google.cloud.dataflow.sdk.io.kinesis.client.response;

import com.google.cloud.dataflow.sdk.repackaged.com.google.common.base.Charsets;

import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;

/**
 * {@link UserRecord} enhanced with utility methods.
 */
public class KinesisRecord extends UserRecord {
    public KinesisRecord(UserRecord record) {
        super(record.isAggregated(),
                record,
                record.getSubSequenceNumber(),
                record.getExplicitHashKey());
    }

    public ExtendedSequenceNumber getExtendedSequenceNumber() {
        return new ExtendedSequenceNumber(getSequenceNumber(), getSubSequenceNumber());
    }

    /***
     * @return unique id of the record based on its position in the stream
     */
    public byte[] getUniqueId() {
        return getExtendedSequenceNumber().toString().getBytes(Charsets.UTF_8);
    }
}

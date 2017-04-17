package com.google.wave.prototype.dataflow.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.wave.prototype.dataflow.model.AggregatedData;

/**
 * Coder for {@link AggregatedData}
 * It just uses AggregatedData.toString() to encode
 * 		AggregatedData.toString() will produce CSV of {@link AggregatedData}
 * In decode,
 * 		CSV is separated into fields by String.split(',') and
 * 		{@link AggregatedData} is constructed using the fields
 */
public class AggregateDataCoder extends AtomicCoder<AggregatedData> {
    private static final long serialVersionUID = 4037984240347308918L;
    private static final int COL_PROPOSAL_ID = 0;
    private static final int COL_OPPORTUNITY_ID = 1;
    private static final int COL_CLICK_COUNT = 2;
    private static final int COL_IMP_COUNT = 3;

    private static final AggregateDataCoder INSTANCE = new AggregateDataCoder();
    private AggregateDataCoder() {	}

    public static AggregateDataCoder getInstance() {
        return INSTANCE;
    }

    @Override
    public void encode(AggregatedData value, OutputStream outStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        // Returning bytes of CSV
        // AggregatedData.toString() will be a CSV
        outStream.write(value.toString().getBytes());
    }

    @Override
    public AggregatedData decode(InputStream inStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        int csvRowSize = inStream.available();
        byte[] csvRow = new byte[csvRowSize];
        inStream.read(csvRow);
        // Stream is converted into String
        // String will be a CSV
        // CSV splitted using comma to get the fields
        // AggregatedData constructed using the fields
        String aggDataStr = new String(csvRow);
        String[] addDataFields = aggDataStr.split(",");


        return new AggregatedData(addDataFields[COL_PROPOSAL_ID],
                addDataFields[COL_OPPORTUNITY_ID],
                Integer.parseInt(addDataFields[COL_CLICK_COUNT]),
                Integer.parseInt(addDataFields[COL_IMP_COUNT]));
    }

}

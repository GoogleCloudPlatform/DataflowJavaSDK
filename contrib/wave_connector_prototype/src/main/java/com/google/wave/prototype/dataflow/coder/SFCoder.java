package com.google.wave.prototype.dataflow.coder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.wave.prototype.dataflow.model.SFReferenceData;

/**
 * Coder for {@link SFReferenceData}
 * It just uses SFReferenceData.toString() to encode
 * 		SFReferenceData.toString() will produce CSV of {@link SFReferenceData}
 * In decode,
 * 		CSV is separated into fields by String.split(',') and
 * 		{@link SFReferenceData} is constructed using the fields
 */
public class SFCoder extends AtomicCoder<SFReferenceData> {
    private static final long serialVersionUID = 4037984240347308918L;
    private static final int COL_ACCOUNT_ID = 0;
    private static final int COL_OPPORTUNITY_ID = 1;
    private static final int COL_PROPOSAL_ID = 2;

    private static final SFCoder INSTANCE = new SFCoder();
    private SFCoder() {	}

    public static SFCoder getInstance() {
        return INSTANCE;
    }

    @Override
    public void encode(SFReferenceData value, OutputStream outStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        // SFReferenceData.toString will provide a String as CSV
        outStream.write(value.toString().getBytes());
    }

    @Override
    public SFReferenceData decode(InputStream inStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        int size = inStream.available();
        byte[] sfRefBytes = new byte[size];
        inStream.read(sfRefBytes);
        String refStr = new String(sfRefBytes);
        String[] sfRefDataFields = refStr.split(",");

        String proposalId = null;
        // Proposal may be null for some rows and hence adding only if it is present
        if (sfRefDataFields.length > 2) {
            proposalId = sfRefDataFields[COL_PROPOSAL_ID];
        }
        return new SFReferenceData(sfRefDataFields[COL_ACCOUNT_ID], sfRefDataFields[COL_OPPORTUNITY_ID], proposalId);
    }

}

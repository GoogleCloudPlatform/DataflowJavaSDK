package com.google.wave.prototype.sf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

public class SFCoder extends AtomicCoder<SFReferenceData> {
    private static final long serialVersionUID = 4037984240347308918L;

    private static final SFCoder INSTANCE = new SFCoder();
    private SFCoder() {	}

    public static SFCoder getInstance() {
        return INSTANCE;
    }

    @Override
    public void encode(SFReferenceData value, OutputStream outStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        outStream.write(value.toString().getBytes());
    }

    @Override
    public SFReferenceData decode(InputStream inStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        int size = inStream.available();
        byte[] data = new byte[size];
        inStream.read(data);
        String refStr = new String(data);
        String[] split = refStr.split(",");

        String proposalId = null;
        if (split.length > 2) {
            proposalId = split[2];
        }
        return new SFReferenceData(split[0], split[1], proposalId);
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

}

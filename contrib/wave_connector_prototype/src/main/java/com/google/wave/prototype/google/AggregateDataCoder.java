package com.google.wave.prototype.google;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

public class AggregateDataCoder extends AtomicCoder<AggregatedData> {
    private static final long serialVersionUID = 4037984240347308918L;

    private static final AggregateDataCoder INSTANCE = new AggregateDataCoder();
    private AggregateDataCoder() {	}

    public static AggregateDataCoder getInstance() {
        return INSTANCE;
    }

    @Override
    public void encode(AggregatedData value, OutputStream outStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        outStream.write(value.toString().getBytes());
    }

    @Override
    public AggregatedData decode(InputStream inStream,
            com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException, IOException {
        int size = inStream.available();
        byte[] data = new byte[size];
        inStream.read(data);
        String refStr = new String(data);
        String[] split = refStr.split(",");

        return new AggregatedData(split[0], split[1], Integer.parseInt(split[2]), Integer.parseInt(split[3]));
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

}

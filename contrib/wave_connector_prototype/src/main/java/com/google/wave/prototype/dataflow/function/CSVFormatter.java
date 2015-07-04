package com.google.wave.prototype.dataflow.function;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.wave.prototype.dataflow.model.AggregatedData;

/**
 * A simple DoFn to convert {@link AggregatedData} into CSV Row
 */
public class CSVFormatter extends DoFn<AggregatedData, String> {
    private static final long serialVersionUID = 398388311953363232L;

    @Override
    public void processElement(DoFn<AggregatedData, String>.ProcessContext c)
            throws Exception {
        StringBuffer sb = new StringBuffer(256);
        sb.append(c.element().toString()).append('\n');
        c.output(sb.toString());
    }

}

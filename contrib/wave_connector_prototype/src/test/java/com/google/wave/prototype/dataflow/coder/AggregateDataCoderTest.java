package com.google.wave.prototype.dataflow.coder;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.wave.prototype.dataflow.BaseTest;
import com.google.wave.prototype.dataflow.model.AggregatedData;

public class AggregateDataCoderTest extends BaseTest {
    private AggregatedData aggregatedData;

    @Before
    public void setup() {
        aggregatedData = new AggregatedData(PROPOSAL_ID_1, OPPOR_ID_1, CLICK_COUNT_1, IMPRESSION_COUNT_1);
    }

    @Test
    public void testCoder() throws Exception {
        ByteArrayOutputStream bos = null;
        ByteArrayInputStream bis = null;
        try {
            AggregateDataCoder coder = AggregateDataCoder.getInstance();

            bos = new ByteArrayOutputStream();
            coder.encode(aggregatedData, bos, Context.NESTED);

            bis = new ByteArrayInputStream(bos.toByteArray());
            AggregatedData decodedAggData = coder.decode(bis, Context.NESTED);

            assertEquals(aggregatedData, decodedAggData);
        } finally {
            if (bos != null) {
                bos.close();
            }
        }
    }
}

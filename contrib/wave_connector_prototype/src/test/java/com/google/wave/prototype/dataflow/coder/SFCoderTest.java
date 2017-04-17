package com.google.wave.prototype.dataflow.coder;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.wave.prototype.dataflow.BaseTest;
import com.google.wave.prototype.dataflow.model.SFReferenceData;

public class SFCoderTest extends BaseTest {
    private SFReferenceData sfReferenceData;

    @Before
    public void setup() {
        sfReferenceData = new SFReferenceData(ACCOUNT_ID_1, OPPOR_ID_1, PROPOSAL_ID_1);
    }

    @Test
    public void testCoder() throws Exception {
        ByteArrayOutputStream bos = null;
        ByteArrayInputStream bis = null;
        try {
            SFCoder coder = SFCoder.getInstance();

            bos = new ByteArrayOutputStream();
            coder.encode(sfReferenceData, bos, Context.NESTED);

            bis = new ByteArrayInputStream(bos.toByteArray());
            SFReferenceData decodedsfData= coder.decode(bis, Context.NESTED);

            assertEquals(sfReferenceData, decodedsfData);
        } finally {
            if (bos != null) {
                bos.close();
            }
        }
    }
}

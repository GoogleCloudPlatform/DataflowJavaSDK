package com.google.wave.prototype.dataflow.transform;

import org.junit.Before;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.model.SFQuery;
import com.google.wave.prototype.dataflow.transform.SFRead;
import com.google.wave.prototype.dataflow.util.SFConstants;

public class SFReadTest {
    private SFQuery sfQuery;

    @Before
    public void setup() throws Exception {
        String sfQueryStr = "SELECT AccountId, Id, ProposalID__c FROM Opportunity where ProposalID__c != null";

        StringBuilder sb = new StringBuilder();
        sb.append(SFConstants.LOCAL_FILE_PREFIX);
        sb.append(System.getProperty("user.dir"));
        sb.append("/test_sf_config.json");

        SFConfig sfConfig = SFConfig.getInstance(sb.toString(), PipelineOptionsFactory.create());

        sfQuery = new SFQuery(sfQueryStr, sfConfig);
    }

    @Test
    public void pTransformTest() {
        TestPipeline pipeline = TestPipeline.create();

        PCollection<SFQuery> input = pipeline.apply(Create.of(sfQuery));
        PCollection<String> results = input.apply(new SFRead());

        DataflowAssert.that(results).containsInAnyOrder("001B0000003oUqJIAU,006B0000002nBrQIAU,0001");
    }
}

package com.google.wave.prototype.dataflow.transform;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.wave.prototype.dataflow.BaseTest;
import com.google.wave.prototype.dataflow.sf.SFSOQLExecutor;
import com.sforce.soap.enterprise.sobject.Opportunity;
import com.sforce.soap.enterprise.sobject.SObject;

public class SFReadTest extends BaseTest {
    private static final String sfQueryStr = "SELECT AccountId, Id, ProposalID__c FROM Opportunity where ProposalID__c != null";

    private SFSOQLExecutor sfSOQLExecutor;

    @Before
    public void setup() throws Exception {
        sfSOQLExecutor = mock(SFSOQLExecutor.class, withSettings().serializable());

        OpportunityExt oppor = new OpportunityExt();
        oppor.setAccountId(ACCOUNT_ID_1);
        oppor.setId(OPPOR_ID_1);
        oppor.setProposalID__c(PROPOSAL_ID_1);
        List<SObject> sobjects = new ArrayList<SObject>();
        sobjects.add(oppor);

        when(sfSOQLExecutor.executeQuery(sfQueryStr)).thenReturn(sobjects);
    }

    @Ignore("Not able to serialize Opportunity, hence not able to mock it. But unit test for SFRead is covered as part SFSOQLExecutor")
    @Test
    public void pTransformTest() {
        TestPipeline pipeline = TestPipeline.create();

        PCollection<String> input = pipeline.apply(Create.of(sfQueryStr));
        PCollection<String> results = input.apply(new SFRead(sfSOQLExecutor));

        DataflowAssert.that(results).containsInAnyOrder(getAsCSV(ACCOUNT_ID_1, OPPOR_ID_1, PROPOSAL_ID_1));

        pipeline.run();
    }

    public class OpportunityExt extends Opportunity implements Serializable {
        private static final long serialVersionUID = -563793703304651268L;


    }
}

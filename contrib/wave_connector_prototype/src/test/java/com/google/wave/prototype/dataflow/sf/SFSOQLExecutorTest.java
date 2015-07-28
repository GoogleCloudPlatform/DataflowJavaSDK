package com.google.wave.prototype.dataflow.sf;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.wave.prototype.dataflow.BaseTest;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.Opportunity;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * Unit test for {@link SFSOQLExecutor}
 */
public class SFSOQLExecutorTest extends BaseTest {
    private static final String sfQueryStr = "SELECT AccountId, Id, ProposalID__c FROM Opportunity where ProposalID__c != null";

    private SFConfig sfConfig;

    @Before
    public void setup() throws Exception {
        sfConfig = mock(SFConfig.class);

        // Returning our EnterpriseConnection which return a single object during query execution
        when(sfConfig.createEnterpriseConnection()).thenReturn(EnterpriseConnectionExt.getInstance());
    }

    @Test
    public void executeQueryTest() throws Exception {
        int expectedRecordsCount = 1;
        SFSOQLExecutor executor = new SFSOQLExecutor(sfConfig);
        List<SObject> results = executor.executeQuery(sfQueryStr);

        assertNotNull(results);
        assertEquals(results.size(), expectedRecordsCount);
        Opportunity opportunity = (Opportunity) results.get(0);

        assertEquals(ACCOUNT_ID_1, opportunity.getAccountId());
        assertEquals(OPPOR_ID_1, opportunity.getId());
        assertEquals(PROPOSAL_ID_1, opportunity.getProposalID__c());
    }

    public static class EnterpriseConnectionExt extends EnterpriseConnection {

        public static EnterpriseConnectionExt getInstance() throws ConnectionException {
            ConnectorConfig config = new ConnectorConfig();
            config.setUsername("dummy_sf_user");
            config.setPassword("dummy_sf_password");
            config.setManualLogin(true);
            // Salesforce SOAP API checks for /services/Soap/c/
            config.setServiceEndpoint("http://dummysgendpoint/services/Soap/c/");
            return new EnterpriseConnectionExt(config);
        }

        public EnterpriseConnectionExt(ConnectorConfig config)
                throws ConnectionException {
            super(config);
        }

        @Override
        public QueryResult query(String queryString) throws ConnectionException {
            QueryResult queryResult = new QueryResult();

            Opportunity opportunity = new Opportunity();
            opportunity.setAccountId(ACCOUNT_ID_1);
            opportunity.setProposalID__c(PROPOSAL_ID_1);
            opportunity.setId(OPPOR_ID_1);

            queryResult.setRecords(new SObject[] {opportunity});
            queryResult.setDone(true);
            return queryResult;
        }

        @Override
        public void logout() throws ConnectionException {
            // no op
        }
    }
}

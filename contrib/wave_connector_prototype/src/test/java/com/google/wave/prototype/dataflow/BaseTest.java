package com.google.wave.prototype.dataflow;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableRow;
import com.google.wave.prototype.dataflow.model.AggregatedData;
import com.google.wave.prototype.dataflow.util.JobConstants;

public class BaseTest {
    // Test data
    protected static final String ACCOUNT_ID_1 = "001B0000003oYAfIAM";
    protected static final String OPPOR_ID_1 = "006B0000002ndnpIAA";
    protected static final String PROPOSAL_ID_1 = "101";
    protected static final int CLICK_COUNT_1 = 100;
    protected static final int IMPRESSION_COUNT_1 = 1000;

    protected static final String ACCOUNT_ID_2 = "001B0000003oYAfIAM";
    protected static final String OPPOR_ID_2 = "006B0000002ndnpIAF";
    protected static final String PROPOSAL_ID_2 = "102";
    protected static final int CLICK_COUNT_2 = 200;
    protected static final int IMPRESSION_COUNT_2 = 2000;

    protected AggregatedData[] getSampleAggDataWithoutOpporId() {
        AggregatedData[] sampleAggData = new AggregatedData[2];

        sampleAggData[0] = new AggregatedData(PROPOSAL_ID_1, CLICK_COUNT_1, IMPRESSION_COUNT_1);
        sampleAggData[1] = new AggregatedData(PROPOSAL_ID_2, CLICK_COUNT_2, IMPRESSION_COUNT_2);

        return sampleAggData;
    }

    protected AggregatedData[] getSampleAggDataWithOpporId() {
        AggregatedData[] sampleAggData = getSampleAggDataWithoutOpporId();

        sampleAggData[0].setOpportunityId(OPPOR_ID_1);
        sampleAggData[1].setOpportunityId(OPPOR_ID_2);

        return sampleAggData;
    }

    protected String getAsCSV(String... columns) {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i != 0) {
                csv.append(',');
            }
            csv.append(columns[i]);
        }
        csv.append('\n');

        return csv.toString();
    }

    protected String getAsCSV(String proposalId, String opporId,
            int clickCount, int impressionCount) {
        return getAsCSV(proposalId, opporId, clickCount + "", impressionCount + "");
    }

    protected TableRow getAsTableRow(String accId1, String opporId1,
            String proposalId1) {
        TableRow row = new TableRow();

        row.set(JobConstants.COL_ACCOUNT_ID, accId1);
        row.set(JobConstants.COL_OPPORTUNITY_ID, opporId1);
        row.set(JobConstants.COL_PROPOSAL_ID, proposalId1);

        return row;
    }

    protected List<TableRow> getSampleSFRefTableRows() {
        List<TableRow> sampleSFRefTableRows = new ArrayList<TableRow>(4);

        sampleSFRefTableRows.add(getAsTableRow(ACCOUNT_ID_1, OPPOR_ID_1, PROPOSAL_ID_1));
        sampleSFRefTableRows.add(getAsTableRow(ACCOUNT_ID_2, OPPOR_ID_2, PROPOSAL_ID_2));

        return sampleSFRefTableRows;
    }
}

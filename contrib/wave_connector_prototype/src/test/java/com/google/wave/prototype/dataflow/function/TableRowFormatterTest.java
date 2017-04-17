package com.google.wave.prototype.dataflow.function;

import static com.google.wave.prototype.dataflow.util.JobConstants.COL_ACCOUNT_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_OPPORTUNITY_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_PROPOSAL_ID;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.wave.prototype.dataflow.BaseTest;

/**
 * Unit test for {@link TableRowFormatter} DoFn
 */
public class TableRowFormatterTest extends BaseTest {

    @Test
    public void formatSFRefTest() {
        TableRowFormatter formatSFRefFn = new TableRowFormatter(getSFRefTableColumns());
        DoFnTester<String,TableRow> doFnTester = DoFnTester.of(formatSFRefFn);

        // Mocking SFRead by manually constructing CSV data
        List<TableRow> results = doFnTester.processBatch(
                getAsCSV(ACCOUNT_ID_1, OPPOR_ID_1, PROPOSAL_ID_1),
                getAsCSV(ACCOUNT_ID_2, OPPOR_ID_2, PROPOSAL_ID_2));

        // Converted tableRows are verified here
        Assert.assertEquals(results, getSampleSFRefTableRows());
    }

    private List<String> getSFRefTableColumns() {
        List<String> columns = new ArrayList<String>(4);

        columns.add(COL_ACCOUNT_ID);
        columns.add(COL_OPPORTUNITY_ID);
        columns.add(COL_PROPOSAL_ID);

        return columns;
    }

}

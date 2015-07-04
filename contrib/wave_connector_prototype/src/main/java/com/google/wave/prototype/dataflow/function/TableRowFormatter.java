package com.google.wave.prototype.dataflow.function;

import java.util.List;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

/**
 * A Google Dataflow DoFn converts the given CSV row into Google BigQuery TableRow
 * Column Names has to be in the order in which the fields are present in CSV
 */
public class TableRowFormatter extends DoFn<String, TableRow> {
    private static final long serialVersionUID = -5798809828662211092L;

    private List<String> columnNames;

    public TableRowFormatter(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
        TableRow row = new TableRow();
        String sfReferenceData = c.element();
        // CSV will contain \n at end
        // \n should be added as column value
        sfReferenceData = removeNewlineChar(sfReferenceData);

        String[] individualFields = sfReferenceData.split(",");
        // Order is according to the query we provide
        // For SELECT AccountId, Id, ProposalID__c FROM Opportunity
        // AccountId will be at 0
        // OpportunityId will be at 1
        // ProposalId will be at 2

        if (columnNames.size() != individualFields.length) {
            throw new Exception ("Number of column does not match with the columns present in CSV");
        }

        int col = 0;
        for (String columnName : columnNames) {
            row.set(columnName, individualFields[col++]);
        }

        c.output(row);
    }

    private String removeNewlineChar(String sfReferenceData) {
        int newlineCharIndex = sfReferenceData.lastIndexOf('\n');
        if (newlineCharIndex != -1) {
            sfReferenceData = sfReferenceData.substring(0, newlineCharIndex);
        }

        return sfReferenceData;
    }
}
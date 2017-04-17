package com.google.wave.prototype.dataflow.function;

import static com.google.wave.prototype.dataflow.util.JobConstants.COL_OPPORTUNITY_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_PROPOSAL_ID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.wave.prototype.dataflow.model.AggregatedData;
import com.google.wave.prototype.dataflow.pipeline.AdDataJob;

/**
 * Enrich AggregatedData with OpportunityId
 * OpportunityId fetched from Google BigQuery for the corresponding ProposalId
 * Google BigQuery TableRow should be provided as sideInput
 */
public class AggregateDataEnricher extends DoFn<AggregatedData, AggregatedData> {
    private static final long serialVersionUID = -369858616535388252L;

    private static final Logger LOG = LoggerFactory.getLogger(AdDataJob.class);

    private PCollectionView<Iterable<TableRow>> sfReferenceDataView;

    public AggregateDataEnricher(PCollectionView<Iterable<TableRow>> sfReferenceDataView) {
        this.sfReferenceDataView = sfReferenceDataView;
    }

    @Override
    public void processElement(
            DoFn<AggregatedData, AggregatedData>.ProcessContext c) throws Exception {
        AggregatedData aggregatedData = c.element();
        String proposalId = aggregatedData.getProposalId();
        // Since in this case BigQuery table considered to be small
        // table rows are passed as sideInput
        Iterable<TableRow> sfReferenceData = c.sideInput(sfReferenceDataView);
        for (TableRow sfReferenceRow : sfReferenceData) {
            String proposalIdFromBigQuery = (String) sfReferenceRow.get(COL_PROPOSAL_ID);
            String opportunityId = (String) sfReferenceRow.get(COL_OPPORTUNITY_ID);
            // Make sure to fetch the opportunityId for the corresponding proposalId
            if (proposalIdFromBigQuery.contains(proposalId)) {
                LOG.info("Adding OpportunityId into aggregatedData : " + opportunityId.toString());
                aggregatedData.setOpportunityId((String) sfReferenceRow.get(COL_OPPORTUNITY_ID));
            }
        }

        c.output(aggregatedData);
    }
}

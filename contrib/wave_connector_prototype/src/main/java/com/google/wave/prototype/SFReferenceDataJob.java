package com.google.wave.prototype;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.wave.prototype.sf.SFReferenceData;
import com.google.wave.prototype.sf.SFSource;

public class SFReferenceDataJob {
    public static class FormatSFRefFn extends DoFn<SFReferenceData, TableRow> {
        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow();
            SFReferenceData sfReferenceData = c.element();
            String proposalId = sfReferenceData.getProposalId();
            row.set("AccountId", sfReferenceData.getAccountId());
            row.set("OpportunityId", sfReferenceData.getOpportunityId());
            if (proposalId != null) {
                row.set("ProposalId", sfReferenceData.getProposalId());
            } else {
                row.set("ProposalId", "");
            }

            c.output(row);
        }
    }

    public static class SFToBigQueryFormatter extends PTransform<PCollection<SFReferenceData>, PCollection<TableRow>> {
        private static final long serialVersionUID = 3238291110118750209L;

        @Override
        public PCollection<TableRow> apply(PCollection<SFReferenceData> sfData) {
            return sfData.apply(ParDo.of(new FormatSFRefFn()));
        }
    }

    private static interface Options extends PipelineOptions {
        @Description("BigQuery table to write to, specified as "
                + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Default.String("gs://sam-bucket1/config/sf_source_config.json")
        String getSfConfigFileLocation();
        void setSfConfigFileLocation(String sfConfigFileLocation);

        @Default.String("SELECT AccountId, Id, proposproposalIdalId__c FROM Opportunity where proposproposalIdalId__c != null")
        String getSfQuery();
        void setSfQuery(String sfQuery);

        @Default.Integer(500)
        int getBatchSize();
        void setBatchSize(int batchSize);
  }

    public static void main(String args[]) throws Exception {
        if (args.length == 0) {
            args = new String[3];
            args[0] = "--project=ace-scarab-94723";
            args[1] = "--stagingLocation=gs://sam-bucket1/staging";
            args[2] = "--output=ace-scarab-94723:SFDCReferenceData.SFRef";
        }

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setRunner(BlockingDataflowPipelineRunner.class);
        Pipeline p = Pipeline.create(options);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("AccountId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("OpportunityId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("ProposalId").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        p.apply(SFSource.readFrom(options.getSfConfigFileLocation(), options.getSfQuery(), options.getBatchSize()))
            .apply(new SFToBigQueryFormatter())
            .apply(BigQueryIO.Write
                    .to(options.getOutput())
                    .withSchema(schema)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run();
    }
}

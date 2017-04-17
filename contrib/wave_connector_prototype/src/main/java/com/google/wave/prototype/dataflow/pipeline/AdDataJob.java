package com.google.wave.prototype.dataflow.pipeline;

import static com.google.wave.prototype.dataflow.util.JobConstants.COL_CLICKS;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_IMPRESSIONS;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_OPPORTUNITY_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_PROPOSAL_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_TYPE_INTEGER;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_TYPE_STRING;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.wave.prototype.dataflow.coder.AggregateDataCoder;
import com.google.wave.prototype.dataflow.function.AggregateDataEnricher;
import com.google.wave.prototype.dataflow.function.CSVFormatter;
import com.google.wave.prototype.dataflow.function.TableRowFormatter;
import com.google.wave.prototype.dataflow.model.AggregatedData;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.sf.SFWaveDatasetWriter;
import com.google.wave.prototype.dataflow.transform.AggregateEvents;
import com.google.wave.prototype.dataflow.transform.SFWaveWrite;

/**
 * Google Dataflow Job
 * 1. Reads the raw Ad Data from Google cloud storage
 * 2. Reads Salesforce Reference data from Google BigQuery
 * 3. Enrich Ad Data using Salesforce Reference data
 * 4. Publish the Enriched data into Salesforce Wave and Google BigQuery
 * To execute, provide the following configuration
 * 		--project=YOUR_PROJECT_ID
 * 		--stagingLocation=YOUR_STAGING_LOCATON
 * 		--inputCSV=GCS_LOCATION_OF_YOUR_RAW_AD_DATA
 * 		--inputTable=GOOGLE_BIGQUERY_TABLE_CONTAINING_SALESFORCE_REFERENCE_DATA
 * 		--output=GOOGLE_BIGQUERY_TABLE_TO_WHICH_ENRICHED_DATA_HAS_TO_BE_ADDED
 * 		--dataset=SALESFORCE WAVE DATASET
 * 		--sfMetadataFileLocation=GCS_LOCATION_OF_SALESFORCE_METADATA_FILE
 * 		--sfConfigFileLocation=GCS_LOCATION_OF_SALESFORCE_CONFIG_FILE
 */
public class AdDataJob {
    public static interface Options extends PipelineOptions {
        @Default.String("gs://sam-bucket1/SampleAdData/ad-server-data1.csv")
        String getInputCSV();
        void setInputCSV(String value);

        @Default.String("ace-scarab-94723:SFDCReferenceData.SFRef")
        String getInputTable();
        void setInputTable(String value);

        @Validation.Required
        @Default.String("ace-scarab-94723:SFDCReferenceData.EnrichedSample")
        String getOutput();
        void setOutput(String value);

        @Default.String("SampleAdDataSet")
        String getDataset();
        void setDataset(String dataset);

        @Default.String("gs://sam-bucket1/SampleAdData/metadata.json")
        String getSfMetadataFileLocation();
        void setSfMetadataFileLocation(String sfMetadataFileLocation);

        @Default.String("gs://sam-bucket1/config/sf_source_config.json")
        String getSfConfigFileLocation();
        void setSfConfigFileLocation(String sfConfigFileLocation);
    }

    private static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(constructTableFieldSchema(COL_PROPOSAL_ID, COL_TYPE_STRING));
        fields.add(constructTableFieldSchema(COL_OPPORTUNITY_ID, COL_TYPE_STRING));
        fields.add(constructTableFieldSchema(COL_CLICKS, COL_TYPE_INTEGER));
        fields.add(constructTableFieldSchema(COL_IMPRESSIONS, COL_TYPE_INTEGER));

        TableSchema tableSchema = new TableSchema().setFields(fields);
        tableSchema.setFields(fields);
        return tableSchema;
    }

    private static TableFieldSchema constructTableFieldSchema(String name, String type) {
        TableFieldSchema tableFieldSchema = new TableFieldSchema();
        tableFieldSchema.setName(name);
        tableFieldSchema.setType(type);

        return tableFieldSchema;
    }

    private static List<String> getEnrichedTableColumns() {
        List<String> columns = new ArrayList<String>(4);

        columns.add(COL_PROPOSAL_ID);
        columns.add(COL_OPPORTUNITY_ID);
        columns.add(COL_CLICKS);
        columns.add(COL_IMPRESSIONS);

        return columns;
    }

    private static SFWaveDatasetWriter createSFWaveDatasetWriter(AdDataJob.Options options) throws Exception {
        SFConfig sfConfig = SFConfig.getInstance(options.getSfConfigFileLocation(), options);
        return new SFWaveDatasetWriter(sfConfig, options.getDataset());
    }

    public static void main(String[] args) throws Exception {
        // Helper if command line options are not provided
        if (args.length < 2) {
            args = new String[2];
            args[0] = "--project=ace-scarab-94723";
            args[1] = "--stagingLocation=gs://sam-bucket1/staging";
        }

        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(Options.class);
        // Always executing using BlockingDataflowPipelineRunner
        options.setRunner(BlockingDataflowPipelineRunner.class);
        Pipeline p = Pipeline.create(options);

        // Reading the CSV present in GCS
        PCollection<AggregatedData> aggregated = p.apply(TextIO.Read.from(options.getInputCSV()))
                .apply(new AggregateEvents())
                .setCoder(AggregateDataCoder.getInstance());

        // Reading Salesforce reference data from Google BigQuery
        PCollection<TableRow> tableColl = p.apply(BigQueryIO.Read.from(options.getInputTable()));
        final PCollectionView<Iterable<TableRow>> sideInput = tableColl.apply(View.<TableRow>asIterable());
        // Salesforce Reference data passed as sideInput
        PCollection<AggregatedData> enriched = aggregated
                .apply(ParDo.withSideInputs(sideInput)
                .of((new AggregateDataEnricher(sideInput))))
                .setCoder(AggregateDataCoder.getInstance());

        // Converting into CSV
        PCollection<String> enrichedCSV = enriched.apply(ParDo.of(new CSVFormatter()));
        // Writing the results into Salesforce Wave
        enrichedCSV
                .apply(new SFWaveWrite(createSFWaveDatasetWriter(options), options.getSfMetadataFileLocation()));

        // Populated BigQuery with enriched data
        enrichedCSV
                .apply(ParDo.of(new TableRowFormatter(getEnrichedTableColumns())))
                .apply(BigQueryIO.Write
                    .to(options.getOutput())
                    .withSchema(getSchema())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        p.run();
    }

}

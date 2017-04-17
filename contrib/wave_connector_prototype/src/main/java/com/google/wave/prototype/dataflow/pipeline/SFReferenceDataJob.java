package com.google.wave.prototype.dataflow.pipeline;

import static com.google.wave.prototype.dataflow.util.JobConstants.COL_ACCOUNT_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_OPPORTUNITY_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_PROPOSAL_ID;
import static com.google.wave.prototype.dataflow.util.JobConstants.COL_TYPE_STRING;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.wave.prototype.dataflow.function.TableRowFormatter;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.sf.SFSOQLExecutor;
import com.google.wave.prototype.dataflow.transform.SFRead;

/**
 * Google Dataflow Job
 * 1. Read Salesforce Reference Data using {@link SFRead}
 * 2. Populate Google BigQuery Table with Salesforce Reference Data
 * To execute, provide the following configuration
 *      --project=YOUR_PROJECT_ID
 *      --stagingLocation=YOUR_STAGING_LOCATON
 *      --output=GOOGLE_BIGQUERY_TABLE_TO_WHICH_SALESFORCE_REFERENCE_DATA_WILL_BE_POPULATED
 *      --sfConfigFileLocation=GCS_LOCATION_OF_SALESFORCE_CONFIG_FILE
 *      --sfQuery=SALESFORCE_SOQL_TO_FETCH_SALESFORCE_REFERENCE_DATA
 */
public class SFReferenceDataJob {

    private static interface Options extends PipelineOptions {
        @Description("BigQuery table to write to, specified as "
                + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Default.String("gs://sam-bucket1/config/sf_source_config.json")
        String getSfConfigFileLocation();
        void setSfConfigFileLocation(String sfConfigFileLocation);

        @Default.String("SELECT AccountId, Id, ProposalID__c FROM Opportunity where ProposalID__c != null")
        String getSfQuery();
        void setSfQuery(String sfQuery);
    }

    private static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();

        fields.add(constructTableFieldSchema(COL_ACCOUNT_ID, COL_TYPE_STRING));
        fields.add(constructTableFieldSchema(COL_OPPORTUNITY_ID, COL_TYPE_STRING));
        fields.add(constructTableFieldSchema(COL_PROPOSAL_ID, COL_TYPE_STRING));

        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    private static TableFieldSchema constructTableFieldSchema(String name, String type) {
        TableFieldSchema tableFieldSchema = new TableFieldSchema();

        tableFieldSchema.setName(name);
        tableFieldSchema.setType(type);

        return tableFieldSchema;
    }

    private static List<String> getSFRefTableColumns() {
        List<String> columns = new ArrayList<String>(4);

        columns.add(COL_ACCOUNT_ID);
        columns.add(COL_OPPORTUNITY_ID);
        columns.add(COL_PROPOSAL_ID);

        return columns;
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 3) {
            args = new String[3];
            args[0] = "--project=ace-scarab-94723";
            args[1] = "--stagingLocation=gs://sam-bucket1/staging";
            args[2] = "--output=ace-scarab-94723:SFDCReferenceData.SFRef";
        }

        Options options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(Options.class);
        options.setRunner(BlockingDataflowPipelineRunner.class);
        Pipeline p = Pipeline.create(options);

        // SFSOQLExecutor which will be used to execute SOQL query
        // SFConfig which will be used to create Salesforce Connection
        SFSOQLExecutor soqlExecutor = new SFSOQLExecutor(SFConfig.getInstance(options.getSfConfigFileLocation(), options));

        // Executing pipeline
        p.apply(Create.of(options.getSfQuery()))
                // Reading from Salesforce
                .apply(new SFRead(soqlExecutor))
                // Convert to TableRow
                .apply(ParDo.of(new TableRowFormatter(getSFRefTableColumns())))
                // Wiring into BigQuery
                .apply(BigQueryIO.Write
                        .to(options.getOutput())
                        .withSchema(getSchema())
                        .withCreateDisposition(
                                BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(
                        // Since all data are fetched from Salesforce,
                        // we need to overwrite the existing data
                                BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run();
    }

}

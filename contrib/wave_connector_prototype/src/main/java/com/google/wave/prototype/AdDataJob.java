package com.google.wave.prototype;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.wave.prototype.google.AggregateDataCoder;
import com.google.wave.prototype.google.AggregatedData;
import com.google.wave.prototype.google.CSVIO;
import com.google.wave.prototype.sf.WaveSink;

public class AdDataJob {
	private static final String USERNAME = "xxxxx@gmail.com";
	private static final String PASSWORD = "xxxxkACjShJk7Uq4WdFJwcADAVRI";

	private static final Logger LOG = LoggerFactory.getLogger(AdDataJob.class);
	 
	public static interface Options extends PipelineOptions {
	    @Default.String("gs://sam-bucket1/SampleAdData/ad-server-data1.csv")
	    String getInputCSV();
	    void setInputCSV(String value);
	    
	    @Default.String("ace-scarab-94723:SFDCReferenceData.SFRef")
	    String getInputTable();
	    void setInputTable(String value);
	    
	    @Description("BigQuery table to write to, specified as "
				+ "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
		@Validation.Required
		@Default.String("ace-scarab-94723:SFDCReferenceData.EnrichedSample")
		String getOutput();
		void setOutput(String value);

		@Default.String(USERNAME)
		String getSFUserId();
		void setSFUserId(String value);

		@Default.String(PASSWORD)
		String getSFPassword();
		void setSFPassword(String value);
		
		@Default.String("sampleAdDataSet")
		String getDataset();
		void setDataset(String dataset);

		@Default.String("sam-bucket1/SampleAdData")
		String getMetadataBucket();
		void setMetadataBucket(String metadataBucket);

		@Default.String("enriched_data.json")
		String getMetadataFile();
		void setMetadataFile(String metadataBucket);
	}

	private static class FilterRawData extends DoFn<String, KV<String, String>> {
		private static final long serialVersionUID = 6002612407682561915L;

		@Override
		public void processElement(
				DoFn<String, KV<String, String>>.ProcessContext c)
				throws Exception {
			String row = c.element();
			String[] split = row.split(",");
			c.output(KV.of(split[10], split[7]));
		}
		
	}
	
	private static class CountEvents extends DoFn<KV<String, Iterable<String>>, AggregatedData> {
		private static final long serialVersionUID = 6002612407682561915L;

		@Override
		public void processElement(
				DoFn<KV<String, Iterable<String>>, AggregatedData>.ProcessContext c) throws Exception {
			KV<String, Iterable<String>> row = c.element();
			Iterable<String> events = row.getValue();
			int clicks = 0;
			int impressions = 0;
			for (String event : events) {
				if (event.equalsIgnoreCase("impression")) {
					impressions++;
				} else if (event.equalsIgnoreCase("click")) {
					clicks++;
				}
			}
			
			c.output(new AggregatedData(row.getKey(), clicks, impressions));
		}
	}

	private static class AggregateEvents extends PTransform<PCollection<String>, PCollection<AggregatedData>> {
		private static final long serialVersionUID = 3238291110118750209L;
		
		@Override
	    public PCollection<AggregatedData> apply(PCollection<String> rawdata) {
			PCollection<KV<String, String>> filteredData = rawdata.apply(ParDo.of(new FilterRawData()));
			PCollection<KV<String, Iterable<String>>> groupedData = filteredData.apply(GroupByKey.<String, String>create());
			return groupedData.apply(ParDo.of(new CountEvents()));
	    }
	}
	
	public static class AddOpportunityId extends DoFn<AggregatedData, AggregatedData> {
		private static final long serialVersionUID = -369858616535388252L;
		private PCollectionView<Iterable<TableRow>> view;
		
		public AddOpportunityId(PCollectionView<Iterable<TableRow>> view) {
			this.view = view;
		}
		
		@Override
		public void processElement(
				DoFn<AggregatedData, AggregatedData>.ProcessContext c) throws Exception {
			AggregatedData aggregatedData = c.element();
			String proposalId = aggregatedData.getProposalId();
			// TODO : This has to be enhanced to read the BigQuery with the proposal Id instead of iterating
			// Not sure which is efficient. Query using a source and using it as sideInput?
			// or querying for each row with proposal Id in worker
			Iterable<TableRow> tableRows = c.sideInput(view);
			for (TableRow tableRow : tableRows) {
				String proposalIdFromBigQuery = (String) tableRow.get("ProposalId");
				String opportunityId = (String) tableRow.get("OpportunityId");
				if (proposalIdFromBigQuery.contains(proposalId)) {
					LOG.info("Storing OpportunityId into aggregatedData : " + opportunityId.toString());
					aggregatedData.setOpportunityId((String) tableRow.get("OpportunityId"));
				}
			}

			c.output(aggregatedData);
		}
	}	
	
	private static class FormatAggData extends DoFn<AggregatedData, TableRow> {
		private static final long serialVersionUID = -369858616535388252L;
		
		@Override
		public void processElement(
				DoFn<AggregatedData, TableRow>.ProcessContext c) throws Exception {
			AggregatedData aggregatedData = c.element();
			c.output(aggregatedData.asTableRow());
		}
	}
	
	private static class TransformAsCSV extends DoFn<AggregatedData, String> {
		private static final long serialVersionUID = 398388311953363232L;

		@Override
		public void processElement(DoFn<AggregatedData, String>.ProcessContext c) throws Exception {
			StringBuffer sb = new StringBuffer(256);
			sb.append(c.element().toString()).append("\n");
			c.output(sb.toString());
		}
		
	}
	
	public static void main(String[] args) {
		// Helper if command line options are not provided
		if (args.length == 0) {
			args = new String[2];
			args[0] = "--project=ace-scarab-94723";
			args[1] = "--stagingLocation=gs://sam-bucket1/staging";
		}
		
		Options options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(Options.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		Pipeline p = Pipeline.create(options);
		
		PCollection<AggregatedData> aggregated = p.apply(CSVIO.CSVSource.readFrom(options.getInputCSV()))
			.apply(new AggregateEvents()).setCoder(AggregateDataCoder.getInstance());
		
		PCollection<TableRow> tableColl = p.apply(BigQueryIO.Read.from(options.getInputTable()));
		final PCollectionView<Iterable<TableRow>> sideInput = tableColl.apply(View.<TableRow>asIterable());
		PCollection<AggregatedData> result = aggregated.apply(ParDo.withSideInputs(sideInput)
				.of((new AddOpportunityId(sideInput)))).setCoder(AggregateDataCoder.getInstance());

		PCollection<String> csvPCollection = result.apply(ParDo.of(new TransformAsCSV()));
		// To check the results. For temp
		csvPCollection.apply(CSVIO.CSVSink.writeTo("gs://sam-bucket1/output/result", "csv"));
		csvPCollection.apply(WaveSink.writeTo());
		
	    // Build the table schema for the output table.
	    List<TableFieldSchema> fields = new ArrayList<>();
	    fields.add(new TableFieldSchema().setName("proposalId").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("opportunityId").setType("STRING"));
	    fields.add(new TableFieldSchema().setName("clicks").setType("INTEGER"));
	    fields.add(new TableFieldSchema().setName("impressions").setType("INTEGER"));
	    TableSchema schema = new TableSchema().setFields(fields);
		result.apply(ParDo.of(new FormatAggData()))
	    	.apply(BigQueryIO.Write
	    	        .to(options.getOutput())
	    	        .withSchema(schema)
	    	        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
	    	        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
	    p.run();
	}
}

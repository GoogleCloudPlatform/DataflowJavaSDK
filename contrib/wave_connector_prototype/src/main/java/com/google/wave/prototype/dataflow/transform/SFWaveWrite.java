package com.google.wave.prototype.dataflow.transform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.model.SFWaveWriteResult;
import com.google.wave.prototype.dataflow.sf.SFWaveDatasetWriter;
import com.google.wave.prototype.dataflow.util.FileUtil;

/**
 * PTransform to write the dataset content into SF Wave This uses Salesforce
 * SOAP API (Partner WSDL) to publish data into Salesforce Wave This PTransform
 * requires the following input {@link SFWaveDatasetWriter} - Writer with
 * {@link SFConfig} which will be used by this transform sfMetadataFileLocation
 * - A Salesforce wave metadata file describing the data to be published to wave
 * Can be a local file or GS file Refer
 * https://resources.docs.salesforce.com/sfdc
 * /pdf/bi_dev_guide_ext_data_format.pdf
 */
public class SFWaveWrite extends
		PTransform<PCollection<String>, PCollection<SFWaveWriteResult>> {
	private static final long serialVersionUID = 5830880169795002498L;
	private static final Logger LOG = LoggerFactory
			.getLogger(SFWaveWrite.class);

	private final SFWaveDatasetWriter writer;
	private final String sfMetadataFileLocation;

	public SFWaveWrite(SFWaveDatasetWriter writer, String sfMetadataFileLocation) {
		this.writer = writer;
		this.sfMetadataFileLocation = sfMetadataFileLocation;
	}

	@Override
	public PCollection<SFWaveWriteResult> apply(PCollection<String> rowData) {
		LOG.debug("SFWaveWrite starts");
		// Number of bundles calculated here
		PCollection<Integer> noOfBundles = rowData
				.apply(new CalculateNoOfBundles());
		PCollectionView<Integer> sideInput = noOfBundles.apply(View
				.<Integer> asSingleton());
		// Making KV with hash modulo as key and CSV row as value
		PCollection<KV<Integer, String>> kvData = rowData
				.apply(ParDo.withSideInputs(sideInput).of(
						new DistributeRowData(sideInput)));
		// Creating bundles using GroupByKey
		PCollection<KV<Integer, Iterable<String>>> groupedRows = kvData
				.apply(GroupByKey.<Integer, String> create());
		// Writing Data into Salesforce Wave
		PCollection<SFWaveWriteResult> writeResult = groupedRows.apply(ParDo
				.of(new Write(writer, sfMetadataFileLocation)));

		LOG.debug("SFWaveWrite ends");
		return writeResult;
	}

	/**
	 * Calculates the Number of bundles to be created Calculation is based on
	 * the size of the data to be sent to Salesforce Wave Size of the data is
	 * calculated using {@code String.length()} and then {@code Sum.SumLongFn}
	 */
	public static class CalculateNoOfBundles extends
			PTransform<PCollection<String>, PCollection<Integer>> {
		private static final long serialVersionUID = -7383871712471335638L;
		private static final String INDIVIDUAL_SIZE_PAR_DO_NAME = "IndividualSize";
		private static final String NO_OF_BUNDLES_PAR_DO_NAME = "NoOfBundles";

		@Override
		public PCollection<Integer> apply(PCollection<String> input) {
			return input.apply(ParDo.named(INDIVIDUAL_SIZE_PAR_DO_NAME).of(

			new DoFn<String, Long>() {
				private static final long serialVersionUID = -6374354958403597940L;

				@Override
				public void processElement(ProcessContext c) throws Exception {
					// String.length is used to get the size of data for an
					// individual row
					// As further grouping takes place, the additional size for
					// UTF-16 characters are ignored
					String rowToBePersisted = c.element();
					c.output(Integer.valueOf(rowToBePersisted.length())
							.longValue());
				}
			}))
			// Calculating the total size of the data to be persisted into
			// Salesforce Wave
					.apply(Combine.globally(new Sum.SumLongFn()))
					// Number of bundles calculated based on the size of data
					.apply(ParDo.named(NO_OF_BUNDLES_PAR_DO_NAME).of(
							new BundleCount()));
		}
	}

	/**
	 * Count the number of bundles to be created Number of bundles to be created
	 * is based on the size of the data to be persisted into Salesforce wave At
	 * a max Saleforce can accept 10MB So size of a bundle should not be more
	 * than 10MB
	 */
	public static class BundleCount extends DoFn<Long, Integer> {
		private static final long serialVersionUID = -7446604319456830150L;

		@Override
		public void processElement(DoFn<Long, Integer>.ProcessContext c)
				throws Exception {
			// No of Bundles = totalSize / (1024 * 1024 * 10)
			// 1024 * 1024 is to convert into MB
			// Maximum support in Salesforce Wave API is 10 MB
			// For example, if the size of the data is 335544320, then 33
			// bundles will be created
			// Math.round(335544320/(1024 * 1024 * 10)) + 1 = 33
			Long totalDataSize = c.element();
			Long maxBundleSize = 1024 * 1024 * 10l;
			if (totalDataSize > maxBundleSize) {
				c.output(Math.round(totalDataSize / maxBundleSize) + 1);
			} else {
				// As the size less than 10MB the data can be handled in single
				// bundle itself
				c.output(1);
			}
		}

	}

	/**
	 * Distributes the data evenly to bundles If the data is of size 32 MB then
	 * data will be distributed to 4 bundles of 8MB each
	 */
	public static class DistributeRowData extends
			DoFn<String, KV<Integer, String>> {
		private static final long serialVersionUID = 3917848069436988535L;
		private PCollectionView<Integer> noOfBundlesPCol;

		// Number of bundles is calculated in CalculateNoOfBundles and
		// provided here as sideInput
		public DistributeRowData(PCollectionView<Integer> noOfBundles) {
			this.noOfBundlesPCol = noOfBundles;
		}

		@Override
		public void processElement(
				DoFn<String, KV<Integer, String>>.ProcessContext c)
				throws Exception {
			// Getting the number of bundles from sideInput
			Integer noOfBundles = c.sideInput(noOfBundlesPCol);
			String waveCSVData = c.element();
			// Using hash modulo to evenly distribute data across bundles
			int hash = Math.abs(waveCSVData.hashCode() % noOfBundles);
			// Using the hash as key which can be grouped later to create
			// bundles
			c.output(KV.of(hash, waveCSVData));
		}
	}

	/**
	 * DoFn which takes care of writing the datasets into Salesforce Wave This
	 * uses {@link SFWaveDatasetWriter}
	 */
	public static class Write extends
			DoFn<KV<Integer, Iterable<String>>, SFWaveWriteResult> {
		private static final long serialVersionUID = -1875427181542264934L;

		private final SFWaveDatasetWriter writer;
		private final String sfMetadataFileLocation;

		public Write(SFWaveDatasetWriter writer, String sfMetadataFileLocation) {
			this.writer = writer;
			this.sfMetadataFileLocation = sfMetadataFileLocation;
		}

		@Override
		public void processElement(
				DoFn<KV<Integer, Iterable<String>>, SFWaveWriteResult>.ProcessContext c)
				throws Exception {

			// Converting the grouped records into bytes
			KV<Integer, Iterable<String>> groupedRecords = c.element();
			Iterable<String> csvRows = groupedRecords.getValue();
			byte[] datasetData = getAsBytes(csvRows);

			String sfObjId = writer.write(
					getMetadataContent(c.getPipelineOptions()), datasetData);
			SFWaveWriteResult sfWaveWriteResult = new SFWaveWriteResult(sfObjId);
			c.output(sfWaveWriteResult);
		}

		private byte[] getMetadataContent(PipelineOptions options)
				throws Exception {
			String content = FileUtil.getContent(sfMetadataFileLocation,
					options);
			return content.getBytes();
		}

		private byte[] getAsBytes(Iterable<String> waveRows) {
			// Converting all CSV rows into single String which will be
			// published to Salesforce WAVE
			StringBuilder csvRows = new StringBuilder();
			// Row may be like
			// AcccountId,OpportunityId,ClickCount,ImpressionCount
			for (String individualRow : waveRows) {
				csvRows.append(individualRow);
				csvRows.append('\n');
			}

			return csvRows.toString().getBytes();
		}

	}

}

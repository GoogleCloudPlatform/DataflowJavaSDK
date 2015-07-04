package com.google.wave.prototype.dataflow.sink;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Write;
import com.google.cloud.dataflow.sdk.transforms.Write.Bound;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.util.GCSFileUtil;
import com.google.wave.prototype.dataflow.util.SFConstants;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * Google Dataflow Salesforce Wave Sink
 * This Sink can be used to publish the data present in Google dataflow pipeline into Saleforce Wave
 * This uses Salesforce SOAP API (Partner WSDL) to publish data into Salesforce Wave
 * This sink requires the following input
 *     dataset - Name of the dataset in which the CSV data will be persisted
 *     configFileLocation - A salesforce configuration file with salesforce credentials
 *       					Can be a local file or GS file
 *       					Below is the sample content of the file
 *								{@code
 *									{
 *                                   	"userId": <salesforce_acccount_id>,
 *                                      "password": <salesforce_account_password>
 *  								}
 *								}
 *     metadataFileLocation - A Salesforce wave metadata file describing the data to be published to wave
 *     						  	Can be a local file or GS file
 *     							Refer https://resources.docs.salesforce.com/sfdc/pdf/bi_dev_guide_ext_data_format.pdf
 *     								to write a metadata file
 */
public final class SFWaveSink extends Sink<KV<Integer, Iterable<String>>> implements
        SFConstants {
    private static final long serialVersionUID = 7552561454095328797L;
    private static final Logger LOG = LoggerFactory.getLogger(SFWaveSink.class);
    private final String dataset;
    private final String configFileLocation;
    private final String metadataFileLocation;

    public static Bound<KV<Integer, Iterable<String>>> writeTo(String dataset,
            String configFileLocation, String metadataFileLocation) {
        return Write.to(new SFWaveSink(dataset, configFileLocation, metadataFileLocation));
    }

    public SFWaveSink(String dataset, String configFileLocation, String metadataFileLocation) {
        this.configFileLocation = configFileLocation;
        this.dataset = dataset;
        this.metadataFileLocation = metadataFileLocation;
    }

    @Override
    public void validate(PipelineOptions options) {
        // Make sure the dataset has been provided
        if (getDataset() == null) {
            throw new RuntimeException("Salesforce Dataset name has to be provided to push the resulted data");
        }
    }

    @Override
    public com.google.cloud.dataflow.sdk.io.Sink.WriteOperation<KV<Integer, Iterable<String>>, SFWaveWriteResult> createWriteOperation(
            PipelineOptions options) {
        return new SFWaveWriteOperation(this);
    }

    public String getConfigFileLocation() {
        return configFileLocation;
    }

    public String getMetadataFileLocation() {
        return metadataFileLocation;
    }

    public String getDataset() {
        return dataset;
    }

    /**
     * WriteResult class
     * This just holds the Salesforce object Id of the persisted data
     */
    private static class SFWaveWriteResult implements Serializable {
        private static final long serialVersionUID = -7451739773848100070L;

        private String sfObjId;

        public SFWaveWriteResult(String sfObjId) {
            this.sfObjId = sfObjId;
        }

        public String getSfObjId() {
            return sfObjId;
        }
    }

    /**
     * WriteOperation class for Salesforce Wave
     * Constructs {@code SFConfig} using configFileLocation provided in {@code SFWaveSink} and
     * reads the metadafile which will be provided to Writer class
     */
    private class SFWaveWriteOperation extends
            Sink.WriteOperation<KV<Integer, Iterable<String>>, SFWaveWriteResult> {
        private static final long serialVersionUID = -390974735128118993L;
        private SFWaveSink sink;
        private SFConfig sfConfig;
        private byte[] metadata;

        public SFWaveWriteOperation(SFWaveSink sink) {
            this.sink = sink;
        }

        @Override
        public void initialize(PipelineOptions options) throws Exception {
            // Constructing SFConfig
            sfConfig = SFConfig.getInstance(getConfigFileLocation(), options);

            GCSFileUtil gcsFileUtil = new GCSFileUtil(options);
            metadata = gcsFileUtil.read(getMetadataFileLocation());
        }

        @Override
        public void finalize(Iterable<SFWaveWriteResult> writerResults,
                PipelineOptions options) throws Exception {
            // Just logging the results.
            // Nothing needs to be done here as everything is written by Writer itself
            for (SFWaveWriteResult sfWaveWriteResult : writerResults) {
                LOG.info("Data has been published to Wave with Salesforce object id " + sfWaveWriteResult.getSfObjId());
            }
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink.Writer<KV<Integer, Iterable<String>>, SFWaveWriteResult> createWriter(
                PipelineOptions options) throws Exception {
            return new SFWaveWriter(this);
        }

        @Override
        public Sink<KV<Integer, Iterable<String>>> getSink() {
            return sink;
        }

        @Override
        public Coder<SFWaveWriteResult> getWriterResultCoder() {
            return SerializableCoder.of(SFWaveWriteResult.class);
        }

        public SFConfig getSFConfig() {
            return this.sfConfig;
        }

        public byte[] getMetadata() {
            return metadata;
        }

    }

    /**
     * Writer class which takes care of writing the datasets into Salesforce Wave
     * In open(),
     * 				Salesforce Wave Connection is created
     * 				Metadata published to Salesforce Wave
     * In write(),
     * 				Convert the grouped record into CSV rows by adding \n at end
     * 				Push the data into Salesforce Wave using the parentId from Id returned
     * 					from metadata publish
     * In close(),
     * 				Finalize the Salesforce publish
     * 				Close salesforce wave connection
     *
     * This uses Salesforce SOAP API (Partner WSDL)
     */
    private class SFWaveWriter extends
            Sink.Writer<KV<Integer, Iterable<String>>, SFWaveWriteResult> {
        private PartnerConnection connection;
        private String parentId;
        private SFWaveWriteOperation writeOperation;

        public SFWaveWriter(SFWaveWriteOperation writeOperation) {
            this.writeOperation = writeOperation;
        }

        @Override
        public void open(String uId) throws Exception {
            SFConfig sfConfig = writeOperation.getSFConfig();
            // Using Salesforce SOAP API Partner.wsdl
            connection = createConnection(sfConfig.getUserId(), sfConfig.getPassword());
            parentId = publishMetaData();
        }

        @Override
        public void write(KV<Integer, Iterable<String>> groupedRecords) throws Exception {
            Iterable<String> csvRows = groupedRecords.getValue();
            byte[] dataToBeWritten = getAsBytes(csvRows);
            publish(dataToBeWritten);
        }

        @Override
        public SFWaveWriteResult close() throws Exception {
            try {
                // Finalizing Wave publish for this worker
                finalizeWavePublish();
            } finally {
                connection.logout();
            }

            // Metadata objectId is returned to SFWaveWriteResult
            return new SFWaveWriteResult(parentId);
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink.WriteOperation<KV<Integer, Iterable<String>>, SFWaveWriteResult> getWriteOperation() {
            return writeOperation;
        }

        private void finalizeWavePublish() throws Exception {
            SObject metaDataSObject = new SObject();
            metaDataSObject.setType(STR_INSIGHTS_EXTERNAL_DATA);
            // Action set to process, which should finalize the DataPart published so on
            metaDataSObject.setField(STR_ACTION, STR_ACTION_PROCESS);
            // Using the Object Id during metadata publish
            metaDataSObject.setId(parentId);

            SaveResult[] metadataPublishResults = connection.update(new SObject[] {metaDataSObject});
            checkResults(metadataPublishResults);
        }

        private void publish(byte[] content) throws Exception {
            // Contents are being pushed here
            SObject dataSObject = new SObject();
            dataSObject.setType(STR_INSIGHTS_EXTERNAL_DATA_PART);
            dataSObject.setField(STR_DATAFILE, content);
            LOG.trace("Writing this data into WAVE : " + new String(content));
            dataSObject.setField(STR_INSIGHTS_EXTERNAL_DATA_ID, parentId);
            // Since the each bundle is max of 10 MB we will have only one part
            // Hence part number is always set to 1
            dataSObject.setField(STR_PART_NUMBER, 1);

            SaveResult[] dataPartPublishResults = connection.create(new SObject[] { dataSObject });
            checkResults(dataPartPublishResults);
        }

        private String publishMetaData() throws Exception {
            // Metadata of a dataset is being published here
            SObject metadataSObject = new SObject();
            metadataSObject.setType(STR_INSIGHTS_EXTERNAL_DATA);
            metadataSObject.setField(STR_FORMAT, STR_CSV_FORMAT);
            metadataSObject.setField(STR_EDGEMART_ALIAS, getDataset());
            metadataSObject.setField(STR_METADATA_JSON, writeOperation.getMetadata());
            metadataSObject.setField(STR_OPERATION, STR_OVERWRITE_OPERATION);
            // Action is None here. It will be Process only after all data part has been created
            metadataSObject.setField(STR_ACTION, STR_ACTION_NONE);

            SaveResult[] metadataPublishResults = connection.create(new SObject[] { metadataSObject });
            return checkResults(metadataPublishResults);
        }

        private String checkResults(SaveResult[] publishResults) throws Exception {
            for (SaveResult publishResult : publishResults) {
                if (publishResult.isSuccess()) {
                    LOG.debug("Flushed to wave : " + publishResult.getId());
                    return publishResult.getId();
                } else {
                    StringBuilder sfWaveErrMsg = new StringBuilder();
                    sfWaveErrMsg.append("Error while flushing data to wave.\n");
                    sfWaveErrMsg.append("Salesforce Job Id : " + publishResult.getId() + "\n");
                    sfWaveErrMsg.append("Salesforce error message : ");
                    // Errors are concatenated to get a meaning message
                    Error[] errors = publishResult.getErrors();
                    for (int i = 0; i < errors.length; i++) {
                        sfWaveErrMsg.append(errors[i].getMessage());
                    }

                    LOG.error(sfWaveErrMsg.toString());

                    // Stopping Job if publish fails
                    throw new Exception(sfWaveErrMsg.toString());
                }
            }

            return null;
        }

        private PartnerConnection createConnection(String sfUserId,
                String sfPassword) throws Exception {
            ConnectorConfig config = new ConnectorConfig();
            LOG.debug("options.getSFUserId() : " + sfUserId);
            config.setUsername(sfUserId);
            config.setPassword(sfPassword);
            try {
                return Connector.newConnection(config);
            } catch (ConnectionException ce) {
                LOG.error("Exception while creating connection", ce);
                throw new Exception(ce);
            }
        }

        private byte[] getAsBytes(Iterable<String> waveRows) {
            // Converting all CSV rows into single String which will be published to Salesforce WAVE
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

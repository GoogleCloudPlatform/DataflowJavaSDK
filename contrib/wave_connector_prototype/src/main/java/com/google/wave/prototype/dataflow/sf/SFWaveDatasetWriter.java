package com.google.wave.prototype.dataflow.sf;

import static com.google.wave.prototype.dataflow.util.SFConstants.STR_ACTION;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_ACTION_NONE;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_ACTION_PROCESS;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_CSV_FORMAT;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_DATAFILE;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_EDGEMART_ALIAS;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_FORMAT;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_INSIGHTS_EXTERNAL_DATA;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_INSIGHTS_EXTERNAL_DATA_ID;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_INSIGHTS_EXTERNAL_DATA_PART;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_METADATA_JSON;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_OPERATION;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_OVERWRITE_OPERATION;
import static com.google.wave.prototype.dataflow.util.SFConstants.STR_PART_NUMBER;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.wave.prototype.dataflow.model.SFConfig;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;

/**
 * This can be used to write metadata and datasetData into SF Wave
 * 1. It creates connection using {@link SFConfig}
 * 2. Writes specified Metadata
 * 3. Writes Dataset data
 * 4. Finalize the write
 * This uses Salesforce SOAP API (Partner WSDL)
 */
public class SFWaveDatasetWriter implements Serializable {
    private static final long serialVersionUID = 5714980864384207026L;

    private static final Logger LOG = LoggerFactory.getLogger(SFWaveDatasetWriter.class);

    private SFConfig sfConfig;
    private String datasetName;

    public SFWaveDatasetWriter(SFConfig sfConfig, String datasetName) {
        this.sfConfig = sfConfig;
        this.datasetName = datasetName;
    }

    public String write(byte[] metadata, byte[] datasetData) throws Exception {
        PartnerConnection connection = null;
        try {
            connection = sfConfig.createPartnerConnection();
            String parentId = publishMetaData(metadata, connection);
            publish(datasetData, parentId, connection);
            finalizeWavePublish(parentId, connection);

            return parentId;
        } finally {
            if (connection != null) {
                connection.logout();
            }
        }
    }

    private void publish(byte[] content, String parentId, PartnerConnection connection) throws Exception {
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


    private void finalizeWavePublish(String parentId, PartnerConnection connection) throws Exception {
        SObject metaDataSObject = new SObject();
        metaDataSObject.setType(STR_INSIGHTS_EXTERNAL_DATA);
        // Action set to process, which should finalize the DataPart published so on
        metaDataSObject.setField(STR_ACTION, STR_ACTION_PROCESS);
        // Using the Object Id during metadata publish
        metaDataSObject.setId(parentId);

        SaveResult[] metadataPublishResults = connection.update(new SObject[] {metaDataSObject});
        checkResults(metadataPublishResults);
    }

    private String publishMetaData(byte[] metadata, PartnerConnection connection) throws Exception {
        // Metadata of a dataset is being published here
        SObject metadataSObject = new SObject();
        metadataSObject.setType(STR_INSIGHTS_EXTERNAL_DATA);
        metadataSObject.setField(STR_FORMAT, STR_CSV_FORMAT);
        metadataSObject.setField(STR_EDGEMART_ALIAS, datasetName);
        metadataSObject.setField(STR_METADATA_JSON, metadata);
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
}

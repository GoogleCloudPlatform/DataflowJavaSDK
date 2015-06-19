package com.google.wave.prototype.sf;

import java.io.IOException;
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
import com.google.wave.prototype.google.GCSFileUtil;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class SFWaveSink extends Sink<KV<Integer, Iterable<String>>> implements
        SFContants {
    private static final long serialVersionUID = 7552561454095328797L;
    private static final Logger LOG = LoggerFactory.getLogger(SFWaveSink.class);
    private String dataset;
    private String configFileLocation;
    private String metadataFileLocation;

    public SFWaveSink(String dataset, String configFileLocation, String metadataFileLocation) {
        this.configFileLocation = configFileLocation;
        this.dataset = dataset;
        this.metadataFileLocation = metadataFileLocation;
    }

    public static Bound<KV<Integer, Iterable<String>>> writeTo(String dataset,
            String configFileLocation, String metadataFileLocation) {
        return Write.to(new SFWaveSink(dataset, configFileLocation, metadataFileLocation));
    }

    @Override
    public void validate(PipelineOptions options) {
        if (getDataset() == null) {
            throw new RuntimeException("SF Dataset name has to be provided to push the resulted data");
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

    protected class SFWaveWriteOperation extends
            Sink.WriteOperation<KV<Integer, Iterable<String>>, SFWaveWriteResult> {
        private static final long serialVersionUID = -390974735128118993L;
        private SFWaveSink sink;
        private String sfUserId;
        private String sfPassword;
        private byte[] metadata;

        public SFWaveWriteOperation(SFWaveSink sink) {
            this.sink = sink;
        }

        @Override
        public void initialize(PipelineOptions options) throws Exception {
            SFConfig sfConfig = SFConfig.getInstance(getConfigFileLocation(), options);
            this.sfPassword = sfConfig.getPassword();
            this.sfUserId = sfConfig.getUserId();

            GCSFileUtil gcsFileUtil = new GCSFileUtil(options);
            metadata = gcsFileUtil.read(getMetadataFileLocation());
        }

        @Override
        public void finalize(Iterable<SFWaveWriteResult> writerResults,
                PipelineOptions options) throws Exception {

            for (SFWaveWriteResult sfWaveWriteResult : writerResults) {
                LOG.info("Data has been published to Wave with Salesforce object id " + sfWaveWriteResult.getSfObjId());
                System.out.println("Data has been published to Wave with Salesforce object id " + sfWaveWriteResult.getSfObjId());
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

        public String getSfUserId() {
            return this.sfUserId;
        }

        public String getSFPassword() {
            return this.sfPassword;
        }

        public byte[] getMetadata() {
            return metadata;
        }

    }

    private class SFWaveWriter extends
            Sink.Writer<KV<Integer, Iterable<String>>, SFWaveWriteResult> {
        private PartnerConnection connection;
        private String parentId;
        private SFWaveWriteOperation writeOperation;
        private int count = 1;

        public SFWaveWriter(SFWaveWriteOperation writeOperation) {
            this.writeOperation = writeOperation;
        }

        @Override
        public void open(String uId) throws Exception {
            connection = createConnection(writeOperation.getSfUserId(), writeOperation.getSFPassword());
            parentId = publishMetaData();
        }

        @Override
        public void write(KV<Integer, Iterable<String>> value) throws Exception {
            Iterable<String> waveRows = value.getValue();
            byte[] dataToBeWritten = getAsBytes(waveRows);
            publishToWave(dataToBeWritten);
            count++;
        }

        @Override
        public SFWaveWriteResult close() throws Exception {
            finalizeWavePublish();
            connection.logout();

            return new SFWaveWriteResult(parentId);
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink.WriteOperation<KV<Integer, Iterable<String>>, SFWaveWriteResult> getWriteOperation() {
            return writeOperation;
        }

        private void finalizeWavePublish() throws ConnectionException {
            SObject sobj = new SObject();
            sobj.setType(STR_INSIGHT_EXTERNAL_DATA);
            sobj.setField(STR_ACTION, STR_PROCESS_NONE);
            sobj.setId(parentId);

            SaveResult[] results = connection.update(new SObject[] {sobj});
            checkResults(results);
        }

        private void publishToWave(byte[] content) throws ConnectionException {
            SObject sobj = new SObject();
            sobj.setType(STR_INSIGHT_EXTERNAL_DATA_PART);
            sobj.setField(STR_DATAFILE, content);
            LOG.debug("Writing this data into WAVE : " + new String(content));
            sobj.setField(STR_INSIGHT_EXTERNAL_DATA_ID, parentId);
            sobj.setField(STR_PART_NUMBER, count);

            SaveResult[] results = connection.create(new SObject[] { sobj });
            checkResults(results);
        }

        private String checkResults(SaveResult[] results) {
            for (SaveResult result : results) {
                if (result.isSuccess()) {
                    LOG.info("Flushed to wave : " + result.getId());
                    return result.getId();
                } else {
                    LOG.error("Error while flusing to wave: " + result.getId());
                    Error[] errors = result.getErrors();
                    for (int i = 0; i < errors.length; i++) {
                        LOG.error(errors[i].getMessage());
                    }
                }
            }

            return null;
        }

        private PartnerConnection createConnection(String sfUserId,
                String sfPassword) throws IOException {
            ConnectorConfig config = new ConnectorConfig();
            LOG.info("options.getSFUserId() : " + sfUserId);
            config.setUsername(sfUserId);
            config.setPassword(sfPassword);
            try {
                return Connector.newConnection(config);
            } catch (ConnectionException ce) {
                LOG.error("Exception while creating connection", ce);
                throw new IOException(ce);
            }
        }

        private String publishMetaData() throws Exception {
            SObject sobj = new SObject();
            sobj.setType(STR_INSIGHT_EXTERNAL_DATA);
            sobj.setField(STR_FORMAT, STR_CSV_FORMAT);
            sobj.setField(STR_EDGEMART_ALIAS, getDataset());
            sobj.setField(STR_METADATA_JSON, writeOperation.getMetadata());
            sobj.setField(STR_OPERATION, STR_OVERWRITE_OPERATION);
            sobj.setField(STR_ACTION, STR_ACTION_NONE);

            SaveResult[] results = connection.create(new SObject[] { sobj });
            return checkResults(results);
        }

        private byte[] getAsBytes(Iterable<String> waveRows) {
            StringBuilder rows = new StringBuilder();
            for (String row : waveRows) {
                rows.append(row);
                rows.append('\n');
            }

            return rows.toString().getBytes();
        }
    }
}

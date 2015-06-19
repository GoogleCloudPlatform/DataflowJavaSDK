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
import com.google.wave.prototype.AdDataJob;
import com.google.wave.prototype.AdDataJob.Options;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * @deprecated
 * Use SFWaveSink instead
 */
@Deprecated()
public class WaveSink extends Sink<String> {
    private static final long serialVersionUID = -268349828465752772L;
    private static final Logger LOG = LoggerFactory.getLogger(WaveSink.class);

    public static Write.Bound<String> writeTo() {
        return Write.to(new WaveSink());
    }

    public void validate(PipelineOptions options) {
        //no op
    }

    public Coder<WaveWriteResult> getWriterResultCoder() {
        return SerializableCoder.of(WaveWriteResult.class);
    }

    @Override
    public com.google.cloud.dataflow.sdk.io.Sink.WriteOperation<String, WaveWriteResult> createWriteOperation(
            PipelineOptions options) {
        return new WaveWriteOperation(this);
    }

    public static class WaveWriteResult implements Serializable {
        private static final long serialVersionUID = -7451739773848100070L;

        String content;

        public WaveWriteResult(String content) {
          this.content = content;
        }

        public String getContent() {
            return content;
        }
    }

    public static class WaveWriteOperation extends Sink.WriteOperation<String, WaveWriteResult> {
        private static final long serialVersionUID = -4537236207550082823L;
        private Sink<String> sink;
        private String dataset;

        public WaveWriteOperation(Sink<String> sink) {
            this.sink = sink;
        }

        @Override
        public void initialize(PipelineOptions pipelineOptions) throws Exception {
            Options options = pipelineOptions.as(AdDataJob.Options.class);
            dataset = options.getDataset();
        }

        @Override
        public Coder<WaveWriteResult> getWriterResultCoder() {
            return SerializableCoder.of(WaveWriteResult.class);
        }

        @Override
        public void finalize(Iterable<WaveWriteResult> writerResults,
                PipelineOptions options) throws Exception {
            PartnerConnection connection = createConnection(options);
            String parentId = publishMetaData(connection);
            StringBuffer content = new StringBuffer();
            content.append("ProposalId,OpportunityId,ClickCount,ImpressionCount\n");
            for (WaveWriteResult waveWriteResult : writerResults) {
                content.append(waveWriteResult.getContent());
                LOG.info("Adding below content to be written ", waveWriteResult.getContent());
            }

            publishToWave(connection, parentId, content.toString(), 1);
            finalizeWavePublish(parentId, connection);
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink.Writer<String, WaveWriteResult> createWriter(
                PipelineOptions options) throws Exception {
            return new WaveWriter(this, options);
        }

        @Override
        public Sink<String> getSink() {
            return sink;
        }

        public String getDataset() {
            return this.dataset;
        }

        private PartnerConnection createConnection(PipelineOptions options) throws IOException {
            AdDataJob.Options adJobOptions = options.as(AdDataJob.Options.class);
            ConnectorConfig config = new ConnectorConfig();
//            LOG.info("options.getSFUserId() : " + adJobOptions.getSFUserId());
//            config.setUsername(adJobOptions.getSFUserId());
//            config.setPassword(adJobOptions.getSFPassword());
            try {
                return Connector.newConnection(config);
            } catch (ConnectionException ce) {
                ce.printStackTrace();
                throw new IOException(ce);
            }
        }

        private String publishMetaData(PartnerConnection connection) throws Exception {
            SObject sobj = new SObject();
            sobj.setType("InsightsExternalData");
            sobj.setField("Format", "Csv");
            sobj.setField("EdgemartAlias", dataset);
            sobj.setField("MetadataJson", "");
            // TODO : Currently getting reading the metadata file from GCS
            // So not specifying the metadatajson for now
//			sobj.setField("MetadataJson", getMetadataJSONContent());
            sobj.setField("Operation", "Overwrite");
            sobj.setField("Action", "None");

            SaveResult[] results = connection.create(new SObject[] {sobj});
            for (SaveResult result : results) {
                if (result.isSuccess()) {
                    return result.getId();
                }
            }

            return null;
        }

        private void publishToWave(PartnerConnection connection, String parentId,
                String content, int noOfObjects) throws ConnectionException {
            if (content != null) {
                SObject sobj = new SObject();
                sobj.setType("InsightsExternalDataPart");
                sobj.setField("DataFile", content.getBytes());
                LOG.info("Writing this data into WAVE : " + content);
                sobj.setField("InsightsExternalDataId", parentId);
                sobj.setField("PartNumber", noOfObjects);

                SaveResult[] results = connection.create(new SObject[] {sobj});

                for (SaveResult result : results) {
                    if (result.isSuccess()) {
                        LOG.info("Flushed to wave : "  + result.getId());
                    } else {
                        LOG.info("Error while flusing to wave: "  + result.getId());
                        Error[] errors = result.getErrors();
                        for (int i = 0; i < errors.length; i++) {
                            LOG.info(errors[i].getMessage());
                        }
                    }
                }
            }
        }

        private void finalizeWavePublish(String parentId, PartnerConnection connection) throws ConnectionException {
            SObject sobj = new SObject();
            sobj.setType("InsightsExternalData");
            sobj.setField("Action", "Process");
            sobj.setId(parentId);

            SaveResult[] results = connection.update(new SObject[] {sobj});
            for (SaveResult result : results) {
                if (result.isSuccess()) {
                    LOG.info("Content written into WAVE, Job Id : "  + result.getId());
                } else {
                    LOG.error("Error while writing content into WAVE, Job Id : "  + result.getId());
                    Error[] errors = result.getErrors();
                    for (int i = 0; i < errors.length; i++) {
                        LOG.error(errors[i].getMessage());
                    }
                }
            }

            connection.logout();
        }
    }

    /**
     * This Writer just collects the data in buffer
     * This is done to avoid multiple push to SF Wave
     *
     */
    public static class WaveWriter extends Sink.Writer<String, WaveWriteResult> {
        private StringBuffer content = new StringBuffer();
        private WaveWriteOperation writeOperation;

        public WaveWriter(WaveWriteOperation writeOperation, PipelineOptions options) throws Exception {
            this.writeOperation = writeOperation;
        }

        /*
        private byte[] getMetadataJSONContent() throws IOException {
             GcsService gcsService = GcsServiceFactory.createGcsService(RetryParams.getDefaultInstance());
             GcsFilename gcsFilename = new GcsFilename(adJobOptions.getMetadataBucket(), adJobOptions.getMetadataFile());
             GcsInputChannel readChannel = gcsService.openReadChannel(gcsFilename, 0);
             ByteBuffer metaDataBuff = ByteBuffer.allocate(1024);
             readChannel.read(metaDataBuff);
             return getAsBytes(metaDataBuff);
        }
         */

        @Override
        public void open(String uId) throws Exception {
        }

        @Override
        public void write(String value) throws Exception {
            content.append(value);
        }

        @Override
        public WaveWriteResult close() throws Exception {
            return new WaveWriteResult(content.toString());
        }

        @Override
        public com.google.cloud.dataflow.sdk.io.Sink.WriteOperation<String, WaveWriteResult> getWriteOperation() {
            return writeOperation;
        }

    }
}

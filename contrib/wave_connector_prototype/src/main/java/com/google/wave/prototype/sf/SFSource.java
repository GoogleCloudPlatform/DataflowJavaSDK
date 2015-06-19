package com.google.wave.prototype.sf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.Opportunity;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * Salesforce Source
 * This can be used to read data from Salesforce which can be used for further dataflow processing
 * This requires salesforce query which is used for fetching the SF data
 * This also requires GCS file location where SF credentials will be placed
 */
public class SFSource extends Source<SFReferenceData> {
    private static final long serialVersionUID = 6057206847961817098L;
    private static final Logger LOG = LoggerFactory.getLogger(SFSource.class);

    //Default Salesforce query result size
    private int bundleSize;
    private String sfQuery;
    private SFConfig sfConfig;
    private String configFileLocation;

    public static Read.Bound<SFReferenceData> readFrom(String configFileLocation, String sfQuery) throws IOException {
        return Read.from(new SFSource(configFileLocation, sfQuery, 500));
    }

    public static Read.Bound<SFReferenceData> readFrom(String configFileLocation, String sfQuery, int bundleSize) throws IOException {
        return Read.from(new SFSource(configFileLocation, sfQuery, bundleSize));
    }

    public SFSource(String configFileLocation, String sfQuery, int bundleSize) throws IOException {
        this.sfQuery = sfQuery;
        this.bundleSize = bundleSize;
        this.configFileLocation = configFileLocation;
    }

    public SFSource(SFConfig sfConfig, String sfQuery, int bundleSize) throws IOException {
        this.sfQuery = sfQuery;
        this.bundleSize = bundleSize;
        this.sfConfig = sfConfig;
    }

    public SFConfig getSFConfig() {
        return sfConfig;
    }

    public String getSfQuery() {
        return sfQuery;
    }

    public void setBundleSize(int bundleSize) {
        this.bundleSize = bundleSize;
    }

    public int getBundleSize() {
        return this.bundleSize;
    }

    public String getConfigFileLocation() {
        return configFileLocation;
    }

    @Override
    public List<? extends Source<SFReferenceData>> splitIntoBundles(
            long desiredBundleSizeBytes, PipelineOptions options)
            throws Exception {
        sfConfig = SFConfig.getInstance(getConfigFileLocation(), options);
        List<SFSource> splits = new ArrayList<SFSource>();

        int totalRecordsCount = getCount();
        LOG.info("Total number of records to be read :" + totalRecordsCount);
        if (totalRecordsCount > getBundleSize()) {
            int noOfSources = Math.round(totalRecordsCount / getBundleSize()) + 1;
            int offset = 0;
            for (int i = 0; i < noOfSources; i++) {
                // Splitting based on the Bundle Size using offset
                StringBuilder sb = new StringBuilder(getSfQuery());
                sb.append(" limit ");
                sb.append(getBundleSize());
                sb.append(" offset ");
                sb.append(offset);
                offset = offset + getBundleSize();

                splits.add(new SFSource(getSFConfig(), sb.toString(), getBundleSize()));
            }
        } else {
            splits.add(new SFSource(getSFConfig(), getSfQuery(), getBundleSize()));
        }

        LOG.debug("Sources are splitted into " + splits.size());
        return splits;
    }

    private int getCount() throws ConnectionException {
        int count = 0;
        EnterpriseConnection connection = createSFConnection();
        QueryResult query = connection.query(getSfQuery());
        count = query.getSize();

        return count;
    }

    private EnterpriseConnection createSFConnection()
            throws ConnectionException {
        return createSFConnection(getSFConfig());
    }

    public static EnterpriseConnection createSFConnection(SFConfig sfConfig)
            throws ConnectionException {
        ConnectorConfig config = new ConnectorConfig();
        config.setUsername(sfConfig.getUserId());
        config.setPassword(sfConfig.getPassword());

        EnterpriseConnection connection = Connector.newConnection(config);
        connection.login(sfConfig.getUserId(), sfConfig.getPassword());
        return connection;
    }

    @Override
    public void validate() {
        if (getSfQuery() == null) {
            throw new RuntimeException("Salesforce query not provided");
        }

    }

    public Reader<SFReferenceData> createReader(PipelineOptions options,
            @Nullable ExecutionContext executionContext) throws IOException {
        return new SFReader(this);
    }

    @Override
    public Coder<SFReferenceData> getDefaultOutputCoder() {
        return SFCoder.getInstance();
    }

    public static class SFReader implements Source.Reader<SFReferenceData> {
        private SFSource source;
        private EnterpriseConnection connection;
        private SObject[] records;
        private SFReferenceData current;
        private int index = 0;
        private int size = 0;
        private String sfQuery;

        public SFReader(SFSource source) throws IOException {
            try {
                connection = createSFConnection(source.getSFConfig());
                sfQuery = source.getSfQuery();
            } catch (ConnectionException ce) {
                ce.printStackTrace();
                throw new IOException(ce);
            }
        }

        @Override
        public boolean start() throws IOException {
            try {
                LOG.info("Executing query : " + getSfQuery());
                QueryResult results = connection.query(getSfQuery());
                size = results.getSize();
                if (size > index) {
                    records = results.getRecords();
                    current = constructSFRefData(records[index++]);
                } else {
                    return false;
                }
            } catch (ConnectionException ce) {
                LOG.error("Exception while fetching data from Salesforce", ce);
                throw new IOException(ce);
            }

            return true;
        }

        @Override
        public boolean advance() throws IOException {
            if (size > index) {
                current = constructSFRefData(records[index++]);
                return true;
            }

            return false;
        }

        @Override
        public SFReferenceData getCurrent() throws NoSuchElementException {
            return current;
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
            return Instant.now();
        }

        @Override
        public void close() throws IOException {
            try {
                connection.logout();
            } catch (ConnectionException e) {
                LOG.warn("Exception while closing Salesforce connection", e);
            }
        }

        @Override
        public Source<SFReferenceData> getCurrentSource() {
            return source;
        }

        private SFReferenceData constructSFRefData(SObject sObject) {
            Opportunity opp = (Opportunity) sObject;
            return new SFReferenceData(opp.getAccountId(), opp.getId(), String.valueOf(opp.getProposproposalIdalId__c()));
        }

        public String getSfQuery() {
            return sfQuery;
        }

    }

}

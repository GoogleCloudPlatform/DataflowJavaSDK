package com.google.wave.prototype.dataflow.sf;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.transform.SFRead;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.SObject;

/**
 * Can be used to exeucte a SF SOQL Query
 * It will be executed using the credentials provided in {@link SFConfig}
 */
public class SFSOQLExecutor implements Serializable {
    private static final long serialVersionUID = 296485933905679924L;

    private static final Logger LOG = LoggerFactory.getLogger(SFRead.class);

    private SFConfig sfConfig;

    public SFSOQLExecutor(SFConfig sfConfig) {
        this.sfConfig = sfConfig;
    }

    public List<SObject> executeQuery(String sfQuery) throws Exception {
        EnterpriseConnection connection = null;
        List<SObject> records = new ArrayList<SObject>();

        try {
            connection = sfConfig.createEnterpriseConnection();

            QueryResult result = connection.query(sfQuery);
            // First call results are added here
            records.addAll(Arrays.asList(result.getRecords()));
            String queryLocator = result.getQueryLocator();
            LOG.info("Total number of records to be read :" + result.getSize());

            // Salesforce will not return all the rows in a single shot if the result is huge
            // By default it will return 500 rows per call
            // To fetch further connection.queryMore is used
            // result.isDone() will tell you where all the records have been read
            boolean done = result.isDone();
            while (!done) {
                result = connection.queryMore(queryLocator);
                records.addAll(Arrays.asList(result.getRecords()));

                done = result.isDone();
            }
        } finally {
            if (connection != null) {
                connection.logout();
            }
        }

        return records;
    }
}

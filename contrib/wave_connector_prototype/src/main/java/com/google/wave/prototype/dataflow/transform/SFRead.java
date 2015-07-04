package com.google.wave.prototype.dataflow.transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.model.SFQuery;
import com.google.wave.prototype.dataflow.util.CSVUtil;
import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * PTransform to read the Salesforce object using SOQL
 * Provided SOQL will be executed and the result will be converted into CSV
 * This uses Salesforce SOAP API (Enterprise.wsdl) to execute SOQL
 * A Sample SOQL will look like,
 *		SELECT AccountId, Id FROM Opportunity
 */
public class SFRead extends PTransform<PCollection<SFQuery>, PCollection<String>>{
    private static final long serialVersionUID = -7168554842895484301L;

    private static final Logger LOG = LoggerFactory.getLogger(SFRead.class);

    private final int noOfBundles;

    public SFRead() {
        // Default to 10
        this.noOfBundles = 10;
    }

    public SFRead(int noOfBundles) {
        this.noOfBundles = noOfBundles;
    }

    @Override
    public PCollection<String> apply(PCollection<SFQuery> input) {
        return input
            // Executing SOQL Query
            .apply(ParDo.of(new ExecuteSOQL()))
            // Creating bundles based on the key
            // Key will be hash modulo
            .apply(GroupByKey.<Integer, String>create())
            .apply(ParDo.of(new RegroupRecords()));
    }

    /**
     * Splitting the grouped data as individual records
     */
    private class RegroupRecords extends DoFn<KV<Integer, Iterable<String>>, String> {
        private static final long serialVersionUID = -2126735721477220174L;

        @Override
        public void processElement(
                DoFn<KV<Integer, Iterable<String>>, String>.ProcessContext c)
                throws Exception {
            // Adding the result as individual Salesforce Data
            Iterable<String> sfRefData = c.element().getValue();
            for (String csvRow : sfRefData) {
                c.output(csvRow);
            }
        }

    }

    /**
     * Executes SOQL Query and provides the result as CSV in bundles
     * Result of the SOQL query will be converted into CSV
     * Bundles will be created according to the noOfBundles specified
     */
    private class ExecuteSOQL extends DoFn<SFQuery, KV<Integer, String>> {
        private static final long serialVersionUID = 3227568229914179295L;

        @Override
        public void processElement(
                DoFn<SFQuery, KV<Integer, String>>.ProcessContext c)
                throws Exception {
            SFQuery sfQuery = c.element();
            // Execute SOQL
            List<SObject> sfResults = executeQuery(sfQuery);
            // Convert to CSV
            CSVUtil csvUtil = new CSVUtil(sfQuery.getQuery());
            for (int i = 0, size = sfResults.size(); i < size; i++) {
                String csvRow = csvUtil.getAsCSV(sfResults.get(i));
                // Getting hash Modulo
                int hashModulo = Math.abs(csvRow.hashCode() % noOfBundles);
                c.output(KV.of(hashModulo, csvRow));
            }
        }

        private EnterpriseConnection createConnection(SFConfig sfConfig) throws ConnectionException {
            // Salesforce Enterprise WSDL is used here
            ConnectorConfig config = new ConnectorConfig();
            config.setUsername(sfConfig.getUserId());
            config.setPassword(sfConfig.getPassword());

            EnterpriseConnection connection = Connector.newConnection(config);
            return connection;
        }

        private List<SObject> executeQuery(SFQuery sfQuery) throws ConnectionException {
            EnterpriseConnection connection = null;
            List<SObject> records = new ArrayList<SObject>();

            try {
                connection = createConnection(sfQuery.getSFConfig());

                QueryResult result = connection.query(sfQuery.getQuery());
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
}

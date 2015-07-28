package com.google.wave.prototype.dataflow.transform;

import java.util.List;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.wave.prototype.dataflow.sf.SFSOQLExecutor;
import com.google.wave.prototype.dataflow.util.CSVUtil;
import com.sforce.soap.enterprise.sobject.SObject;

/**
 * PTransform to read the Salesforce object using SOQL
 * SOQL query present in pipeline will be executed and the result will be converted into CSV
 * This uses Salesforce SOAP API (Enterprise.wsdl) to execute SOQL
 * A Sample SOQL will look like,
 *		SELECT AccountId, Id FROM Opportunity
 */
public final class SFRead extends PTransform<PCollection<String>, PCollection<String>>{
    private static final long serialVersionUID = -7168554842895484301L;

    private final int noOfBundles;
    private final SFSOQLExecutor soqlExecutor;

    public SFRead(SFSOQLExecutor soqlExecutor) {
        // Default to 10
        this.noOfBundles = 10;
        this.soqlExecutor = soqlExecutor;
    }

    public SFRead(SFSOQLExecutor soqlExecutor, int noOfBundles) {
        this.noOfBundles = noOfBundles;
        this.soqlExecutor = soqlExecutor;
    }

    @Override
    public PCollection<String> apply(PCollection<String> input) {
        return input
            // Executing SOQL Query
            .apply(ParDo.of(new ExecuteSOQL(soqlExecutor, noOfBundles)))
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
    public static class ExecuteSOQL extends DoFn<String, KV<Integer, String>> {
        private static final long serialVersionUID = 3227568229914179295L;

        private int noOfBundles;
        private SFSOQLExecutor soqlExecutor;

        public ExecuteSOQL(SFSOQLExecutor soqlExecutor, int noOfBundles) {
            this.soqlExecutor = soqlExecutor;
            this.noOfBundles = noOfBundles;
        }

        @Override
        public void processElement(
                DoFn<String, KV<Integer, String>>.ProcessContext c)
                throws Exception {
            String sfQuery = c.element();
            // Execute SOQL
            List<SObject> sfResults = soqlExecutor.executeQuery(sfQuery);
            // Convert to CSV
            CSVUtil csvUtil = new CSVUtil(sfQuery);
            for (int i = 0, size = sfResults.size(); i < size; i++) {
                String csvRow = csvUtil.getAsCSV(sfResults.get(i));
                // Getting hash Modulo
                int hashModulo = Math.abs(csvRow.hashCode() % noOfBundles);
                c.output(KV.of(hashModulo, csvRow));
            }
        }
    }
}

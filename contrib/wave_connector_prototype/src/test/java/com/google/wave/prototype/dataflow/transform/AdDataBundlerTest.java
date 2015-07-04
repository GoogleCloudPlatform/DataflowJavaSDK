package com.google.wave.prototype.dataflow.transform;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.wave.prototype.dataflow.transform.AdDataBundler;
import com.google.wave.prototype.dataflow.transform.AdDataBundler.BundleCount;
import com.google.wave.prototype.dataflow.transform.AdDataBundler.DistributeRowData;

/**
 * Simple unit tests for AdDataBundler PTransform and its DoFn
 */
public class AdDataBundlerTest {

    @Test
    public void calculateNoOfBundlesDoFnTest() {
        BundleCount bundleCtFn = new AdDataBundler.BundleCount();
        DoFnTester<Long, Integer> bundleCtFnTester = DoFnTester.of(bundleCtFn);

        long bundle = 1024 * 1024 * 10l;
        // This should create 2 bundles
        long input1 = bundle + 1;

        // This should create 32 bundles
        long input2 = (bundle * 31) + 1024;

        // These should create 1 bundle
        long input3 = 1024l;
        long input4 = 0l;

        List<Integer> results = bundleCtFnTester.processBatch(input1, input2, input3, input4);
        Assert.assertThat(results, CoreMatchers.hasItems(2, 32, 1, 1));
    }

    @Test
    public void distributeRowDataDoFnTest() {
        int noOfBundles = 2;
        Pipeline p = TestPipeline.create();
        // Preparing sideInput
        PCollection<Integer> bundleCount = p.apply(Create.of(noOfBundles));
        PCollectionView<Integer> sideInput = bundleCount.apply(View.<Integer> asSingleton());
        DistributeRowData distributeRowDataDoFn = new AdDataBundler.DistributeRowData(sideInput);

        DoFnTester<String, KV<Integer, String>> doFnTester = DoFnTester.of(distributeRowDataDoFn);
        // Providing number of bundles as sideInput
        doFnTester.setSideInputInGlobalWindow(sideInput, Arrays.asList(noOfBundles));

        List<KV<Integer, String>> results = doFnTester.processBatch(getSampleSFRefData());
        // Result should have 4 KV with 2 unique keys
        Assert.assertEquals(4, results.size());
        // Checking whether the result has two unique keys as noOfBundles is 2
        Set<Integer> keys = new HashSet<Integer>();
        for (KV<Integer, String> kv : results) {
            keys.add(kv.getKey());
        }

        Assert.assertEquals("Proper number of bundles are not created", noOfBundles, keys.size());
    }

    @Test
    public void adDataBundlerTest() {
        Pipeline p = TestPipeline.create();

        PCollection<String> inputPCol = p.apply(Create.of(getSampleSFRefData()));
        PCollection<KV<Integer, Iterable<String>>> output = inputPCol.apply(new AdDataBundler());

        // Just checking whether one KV is present in the output
        // As the exacct checking is already done in distributeRowDataDoFnTest and calculateNoOfBundlesDoFnTest
        KV<Integer, Iterable<String>> expectedKV = KV.of(0, (Iterable<String>) Arrays.asList(getSampleSFRefData()));
        DataflowAssert.that(output).containsInAnyOrder(Arrays.asList(expectedKV));
    }

    private String[] getSampleSFRefData() {
        String[] sfRefDat = new String[4];
        // accountId, opportunityId, proposalId inputs
        sfRefDat[0] = "001B0000003oYAfIAM,006B0000002ndnpIAA,102";
        sfRefDat[1] = "001B0000003oYAfIAM,006B0000002ndnuIAA,103";
        sfRefDat[2] = "001B0000003oYAfIAM,006B0000002ndnkIAA,101";
        sfRefDat[3] = "001B0000003oUqJIAU,006B0000002nBrQIAU,0001";

        return sfRefDat;
    }

}

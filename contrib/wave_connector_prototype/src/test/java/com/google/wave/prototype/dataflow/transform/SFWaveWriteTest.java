package com.google.wave.prototype.dataflow.transform;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.wave.prototype.dataflow.model.SFWaveWriteResult;
import com.google.wave.prototype.dataflow.sf.SFWaveDatasetWriter;
import com.google.wave.prototype.dataflow.transform.SFWaveWrite.BundleCount;
import com.google.wave.prototype.dataflow.transform.SFWaveWrite.DistributeRowData;
import com.google.wave.prototype.dataflow.transform.SFWaveWrite.Write;
import com.google.wave.prototype.dataflow.util.FileUtil;
import com.google.wave.prototype.dataflow.util.SFConstants;

/**
 * Simple unit tests for {@link SFWaveWrite} {@link PTransform} and its {@link DoFn}
 */
public class SFWaveWriteTest {
    private static final String SAMPLE_DATA_TO_BE_WRITTEN = "001B0000003oYAfIAM,006B0000002ndnpIAA,102";
    private static final String SAMPLE_SF_OBJ_ID = "testSFOBjId";

    private SFWaveDatasetWriter writer;
    private String metadataFileLocation;

    @Before
    public void setup() throws Exception {
        StringBuilder metadataFileLocationSB = new StringBuilder();
        metadataFileLocationSB.append(SFConstants.LOCAL_FILE_PREFIX);
        metadataFileLocationSB.append(System.getProperty("user.dir"));
        metadataFileLocationSB.append("/test_metadata.json");

        metadataFileLocation = metadataFileLocationSB.toString();

        writer = mock(SFWaveDatasetWriter.class, withSettings().serializable());
        when(writer.write(
                FileUtil.getContent(metadataFileLocation.toString(), PipelineOptionsFactory.create()).getBytes(),
                (SAMPLE_DATA_TO_BE_WRITTEN + "\n").getBytes()))
                    .thenReturn(SAMPLE_SF_OBJ_ID);
    }

    @Test
    public void calculateNoOfBundlesDoFnTest() {
        BundleCount bundleCtFn = new SFWaveWrite.BundleCount();
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
        DistributeRowData distributeRowDataDoFn = new SFWaveWrite.DistributeRowData(sideInput);

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

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteDoFn() throws Exception {

        KV<Integer, Iterable<String>> input = KV.of(1, (Iterable<String>) Arrays.asList(SAMPLE_DATA_TO_BE_WRITTEN));

        Write writeDoFn = new SFWaveWrite.Write(writer, metadataFileLocation);
        DoFnTester<KV<Integer,Iterable<String>>,SFWaveWriteResult> doFnTester = DoFnTester.of(writeDoFn);

        // SFWaveDatasetWriter is mocked
        // If proper bytes are sent by SFWaveWrite.Writethe it will return SAMPLE_SF_OBJ_ID
        // So just checking whether it returns SAMPLE_SF_OBJ_ID or not
        List<SFWaveWriteResult> result = doFnTester.processBatch(input);
        Assert.assertThat(result, CoreMatchers.hasItems(new SFWaveWriteResult(SAMPLE_SF_OBJ_ID)));
    }

    @Test
    public void sfWaveWriteTest() {
        Pipeline p = TestPipeline.create();

        PCollection<String> inputPCol = p.apply(Create.of(SAMPLE_DATA_TO_BE_WRITTEN));
        PCollection<SFWaveWriteResult> output = inputPCol.apply(new SFWaveWrite(writer, metadataFileLocation));

        // SFWaveDatasetWriter is mocked
        // If proper bytes are sent by SFWaveWrite.Writethe it will return SAMPLE_SF_OBJ_ID
        // So just checking whether it returns SAMPLE_SF_OBJ_ID or not
        DataflowAssert.that(output).containsInAnyOrder(Arrays.asList(new SFWaveWriteResult(SAMPLE_SF_OBJ_ID)));
        p.run();
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

package com.google.wave.prototype.dataflow.sink;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.wave.prototype.dataflow.sink.SFWaveSink;
import com.google.wave.prototype.dataflow.util.SFConstants;

/**
 * Unit test for SFWaveSinkTest
 * This will not execute the pipeline as Salesforce has upload limit to 20 per day
 */
public class SFWaveSinkTest {
    private String configFileLocation;
    private String metadataFileLocation;
    private String dataset;

    @Before
    public void setup() {
        dataset = "AdDataset_From_Unittests";

        StringBuilder baseLocation = new StringBuilder();
        baseLocation.append(SFConstants.LOCAL_FILE_PREFIX);
        baseLocation.append(System.getProperty("user.dir"));

        configFileLocation = new StringBuilder(baseLocation).append("/test_sf_config.json").toString();
        metadataFileLocation = new StringBuilder(baseLocation).append("/metadata.json").toString();
    }

    @Test
    public void testSink() {
        Pipeline pipeline = TestPipeline.create();

        // Having single bundle
        // So setting the key (hash modulo) to 0
        KV<Integer, Iterable<String>> sampleKV = KV.of(0, getSampleSFWaveData());
        // Creating the pipeline using the sample data
        PCollection<KV<Integer,Iterable<String>>> sfWaveDataPColl = pipeline.apply(Create.of(sampleKV));
        PDone done = sfWaveDataPColl.apply(SFWaveSink.writeTo(dataset, configFileLocation, metadataFileLocation));

        // Not executing the pipeline as Salesforce has upload limit to 20 per day
        Assert.assertNotNull(done);
    }

    private Iterable<String> getSampleSFWaveData() {
        List<String> sampleSFWaveData = new ArrayList<String>(4);
        sampleSFWaveData.add("103,006j000000I3cksAAB,29,131");
        sampleSFWaveData.add("102,006j000000I3ckxAAB,18,95");
        sampleSFWaveData.add("101,006j000000I3cl2AAB,12,72");

        return sampleSFWaveData;
    }
}

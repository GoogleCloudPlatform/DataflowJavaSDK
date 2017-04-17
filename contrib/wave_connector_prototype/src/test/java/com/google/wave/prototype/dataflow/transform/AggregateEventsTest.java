package com.google.wave.prototype.dataflow.transform;

import java.util.Arrays;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.wave.prototype.dataflow.coder.AggregateDataCoder;
import com.google.wave.prototype.dataflow.model.AggregatedData;
import com.google.wave.prototype.dataflow.transform.AggregateEvents;
import com.google.wave.prototype.dataflow.transform.AggregateEvents.CountEvents;
import com.google.wave.prototype.dataflow.transform.AggregateEvents.FilterRawData;

/**
 * Unit tester for AggregateEvents PTransform and the DoFn present in it
 */
public class AggregateEventsTest {

    @SuppressWarnings("unchecked")
    @Test
    public void filterRawDataTest() {
        FilterRawData filterRawDataDoFn = new AggregateEvents.FilterRawData();
        DoFnTester<String, KV<String, String>> doFnTester = DoFnTester.of(filterRawDataDoFn);

        // getAdDataSampleCSVRows() will return raw AdData csv rows
        // FilterRawData DoFn will extract ProposalId and event from it
        List<KV<String, String>> results = doFnTester.processBatch(getAdDataSampleCSVRows());

        // Based on the input following KV are expected
        KV<String, String> expectedValue1 = KV.of("101", "Impression");
        KV<String, String> expectedValue2 = KV.of("102", "Click");
        KV<String, String> expectedValue3 = KV.of("101", "Click");
        Assert.assertThat(results, CoreMatchers.hasItems(expectedValue1, expectedValue2, expectedValue3));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void countEventsDoFnTest() {
        CountEvents countEventsDoFn = new AggregateEvents.CountEvents();
        DoFnTester<KV<String, Iterable<String>>, AggregatedData> countEventDoFnTester = DoFnTester.of(countEventsDoFn);

        // Input to AggregateEvents.CountEvents
        KV<String, Iterable<String>> kvPropsalIdEvents1 = KV.of("101", (Iterable<String>) Arrays.asList("Impression", "Click", "Impression"));
        KV<String, Iterable<String>> kvPropsalIdEvents2 = KV.of("102", (Iterable<String>) Arrays.asList("Click", "Impression"));
        KV<String, Iterable<String>> kvPropsalIdEvents3 = KV.of("103", (Iterable<String>) Arrays.asList("Click"));

        List<AggregatedData> results = countEventDoFnTester.processBatch(kvPropsalIdEvents1, kvPropsalIdEvents2, kvPropsalIdEvents3);

        // Expected results
        // For proposalId 101, there are 1 Click and 2 Impressions in the input
        // Hence the expected in new AggregatedData("101", 1, 2)
        // For proposalId 102, there are 1 Click and 1 Impression in the input
        // For proposalId 103, there are 1 Click and 0 Impression in the input
        AggregatedData expectedValue1 = new AggregatedData("101", 1, 2);
        AggregatedData expectedValue2 = new AggregatedData("102", 1, 1);
        AggregatedData expectedValue3 = new AggregatedData("103", 1, 0);
        Assert.assertThat(results, CoreMatchers.hasItems(expectedValue1, expectedValue2, expectedValue3));
    }

    @Test
    public void aggregateEventsTransformTest() {
        Pipeline p = TestPipeline.create();

        PCollection<String> inPCol = p.apply(Create.of(getAdDataSampleCSVRows()));
        PCollection<AggregatedData> result = inPCol.apply(new AggregateEvents())
                .setCoder(AggregateDataCoder.getInstance());

        // Input data contains 3 rows
        // 2 proposal Id present in input 101 and 102
        // And proposal Id 101 has 1 Impression and 1 Click
        // Proposal Id 102 has 1 Click
        // So expected values are new AggregatedData("101", 1, 1) and new AggregatedData("102", 1, 0)
        AggregatedData expectedValue1 = new AggregatedData("101", 1, 1);
        AggregatedData expectedValue2 = new AggregatedData("102", 1, 0);
        DataflowAssert.that(result).containsInAnyOrder(Arrays.asList(expectedValue1, expectedValue2));

        p.run();
    }

    private String[] getAdDataSampleCSVRows() {
        String[] adDataSampleCSVRows = new String[3];
        adDataSampleCSVRows[0] = "1,01-01-14 9:00,ip-10-150-38-122/10.150.38.122,0,70.209.198.223,http://sample.com,3232,Impression,3,1,101";
        adDataSampleCSVRows[1] = "2,01-01-14 9:01,ip-10-150-38-122/10.150.38.123,0,70.209.198.223,http://sample.com,3232,Click,3,1,102";
        adDataSampleCSVRows[2] = "3,01-01-14 9:00,ip-10-150-38-122/10.150.38.122,0,70.209.198.223,http://sample.com,3232,Click,3,1,101";

        return adDataSampleCSVRows;
    }

}

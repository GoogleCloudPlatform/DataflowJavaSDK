package com.google.wave.prototype.dataflow.function;

import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.wave.prototype.dataflow.BaseTest;
import com.google.wave.prototype.dataflow.model.AggregatedData;

/**
 * Unit tests for {@link AggregateDataEnricher}
 */
public class AggregateDataEnricherTest extends BaseTest {

    @Test
    public void enrichTest() {
        // Creating pipeline to construct sideInput
        TestPipeline testPipeline = TestPipeline.create();
        // Constructing sideInput
        List<TableRow> sampleSFRefTableRows = getSampleSFRefTableRows();
        PCollection<TableRow> sampleSFRefData = testPipeline.apply(Create.of(sampleSFRefTableRows));
        PCollectionView<Iterable<TableRow>> sideInput = sampleSFRefData.apply(View.<TableRow>asIterable());

        AggregateDataEnricher enricher = new AggregateDataEnricher(sideInput);
        DoFnTester<AggregatedData,AggregatedData> doFnTester = DoFnTester.of(enricher);
        doFnTester.setSideInputInGlobalWindow(sideInput, sampleSFRefTableRows);

        // Input Aggregated provided without opportunity Id
        List<AggregatedData> results = doFnTester.processBatch(getSampleAggDataWithoutOpporId());

        // Check whether the result has opportunity id populated with it
        Assert.assertThat(results, CoreMatchers.hasItems(getSampleAggDataWithOpporId()));
    }

}

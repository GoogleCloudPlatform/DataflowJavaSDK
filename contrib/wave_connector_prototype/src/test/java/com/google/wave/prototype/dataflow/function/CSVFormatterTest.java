package com.google.wave.prototype.dataflow.function;

import java.util.List;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.wave.prototype.dataflow.BaseTest;
import com.google.wave.prototype.dataflow.model.AggregatedData;

public class CSVFormatterTest extends BaseTest {

    @Test
    public void transformAsCSVTest() {
        CSVFormatter csvFormatter = new CSVFormatter();
        DoFnTester<AggregatedData, String> dofnTester = DoFnTester.of(csvFormatter);

        List<String> results = dofnTester.processBatch(getSampleAggDataWithOpporId());
        Assert.assertThat(results, CoreMatchers.hasItems(getSampleEnrichedDataAsCSV()));
    }

    private String[] getSampleEnrichedDataAsCSV() {
        String[] sampleEnrichedCSVs= new String[2];

        sampleEnrichedCSVs[0] = getAsCSV(PROPOSAL_ID_1, OPPOR_ID_1, CLICK_COUNT_1, IMPRESSION_COUNT_1);
        sampleEnrichedCSVs[1] = getAsCSV(PROPOSAL_ID_2, OPPOR_ID_2, CLICK_COUNT_2, IMPRESSION_COUNT_2);

        return sampleEnrichedCSVs;
    }
}

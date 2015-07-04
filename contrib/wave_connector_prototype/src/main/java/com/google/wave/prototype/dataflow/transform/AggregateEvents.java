package com.google.wave.prototype.dataflow.transform;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.wave.prototype.dataflow.model.AggregatedData;
import com.google.wave.prototype.dataflow.util.JobConstants;

/**
 * Aggregate the AdData using the proposalId and event present in AdData CSV
 * AdData CSV data is with the below headers,
 *     id,time,local_host,pixel_id,client_ip,request_url,cookie_id,event,version,success_code,proposal_id
 * In this event will be either click or Impression. There will be multiple rows with a single proposal_id
 * This PTransform will transform such rows into {@link AggregateEvents}
 */
public class AggregateEvents extends
        PTransform<PCollection<String>, PCollection<AggregatedData>> {
    private static final long serialVersionUID = 3238291110118750209L;

    @Override
    public PCollection<AggregatedData> apply(PCollection<String> rawdata) {
        // Just selecting ProposalId and events
        PCollection<KV<String, String>> filteredData = rawdata.apply(ParDo
                .of(new FilterRawData()));
        // Grouping all events for a proposalId
        PCollection<KV<String, Iterable<String>>> groupedData = filteredData
                .apply(GroupByKey.<String, String> create());
        // Counting the number of clicks and impressions for a proposalId
        return groupedData.apply(ParDo.of(new CountEvents()));
    }

    /**
     * Construct KV with proposalId as key and event as value for a given CSV Row (AdData)
     * CSV Row will be the input for this DoFn
     * Output will be a KV with proposal_id in the row as key and event in the row as value
     * For example, for the below input
     *     1,01-01-14 9:00,ip-10-150-38-122/10.150.38.122,0,70.209.198.223,http://sample.com,3232,Impression,3,1,101
     * output will be
     *     KV.of(101, Impression)
     */
    protected static class FilterRawData extends DoFn<String, KV<String, String>> {
        private static final long serialVersionUID = 6002612407682561915L;
        private static int COL_PROPOSAL_ID = 10;
        private static int COL_EVENT = 7;

        @Override
        public void processElement(
                DoFn<String, KV<String, String>>.ProcessContext c)
                throws Exception {
            // CSVRow will be like
            // id,time,local_host,pixel_id,client_ip,request_url,cookie_id,event,version,success_code,proposal_id
            // Column 7 and 10. i.e. event and proposal_id
            String csvRow = c.element();
            String[] columns = csvRow.split(JobConstants.STR_COMMA);
            // Result will be KV with proposal_id as key and event as value
            c.output(KV.of(columns[COL_PROPOSAL_ID], columns[COL_EVENT]));
        }

    }

    /**
     * Count the number of clicks and number of Impressions for a specific ProposalId
     * Input for this DoFn will be KV with key as proposalId and value as events. Like,
     *     KV(101, ("Impression", "Impression", "Click")
     * Output will be {@link AggregateEvents} with the proposalId and number of clicks and Impressions
     */
    public static class CountEvents extends
            DoFn<KV<String, Iterable<String>>, AggregatedData> {
        private static final long serialVersionUID = 6002612407682561915L;
        private static final String STR_IMPRESSION = "impression";
        private static final String STR_CLICK = "click";

        @Override
        public void processElement(
                DoFn<KV<String, Iterable<String>>, AggregatedData>.ProcessContext c)
                throws Exception {
            // Element will be like,
            // KV(101, ("Impression", "Impression", "Click")
            KV<String, Iterable<String>> proposalIdEventsKV = c.element();
            // Getting the events alone
            // ("Impression", "Impression", "Click")
            Iterable<String> events = proposalIdEventsKV.getValue();
            int clicks = 0;
            int impressions = 0;
            // Iterating events and increasing the click and impression count
            for (String event : events) {
                if (event.equalsIgnoreCase(STR_IMPRESSION)) {
                    impressions++;
                } else if (event.equalsIgnoreCase(STR_CLICK)) {
                    clicks++;
                }
            }

            // Constructing new AggregatedData with proposalId, Click Count and Impression Count
            c.output(new AggregatedData(proposalIdEventsKV.getKey(), clicks, impressions));
        }
    }
}
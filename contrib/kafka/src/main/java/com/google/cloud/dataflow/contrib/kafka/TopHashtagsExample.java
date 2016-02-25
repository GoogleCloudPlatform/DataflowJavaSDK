package com.google.cloud.dataflow.contrib.kafka;

import java.util.List;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation.Required;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;

/**
 * Every minute, print top English hashtags over 10 last 10 minutes
 * TODO: Move this out this directory.
 */
public class TopHashtagsExample {
  private static final Logger LOG = LoggerFactory.getLogger(TopHashtagsExample.class);

  public static interface Options extends PipelineOptions {
    @Description("Sliding window size, in minutes")
    @Default.Integer(10)
    Integer getSlidingWindowSize();
    void setSlidingWindowSize(Integer value);

    @Description("Trigger window size, in minutes")
    @Default.Integer(1)
    Integer getSlidingWindowPeriod();
    void setSlidingWindowPeriod(Integer value);

    @Description("Bootstarp Server(s) for Kafka")
    @Required
    String getBootstrapServers();
    void setBootstrapServers(String servers);

    @Description("One or more topics to read from")
    @Required
    List<String> getTopics();
    void setTopics(List<String> topics);

    @Description("Number of Top Hashtags")
    @Default.Integer(10)
    Integer getNumTopHashtags();
    void setNumTopHashtags(Integer count);
  }

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  /**
   * Emit each of the hashtags in tweet json
   */
  static class ExtractHashtagsFn extends DoFn<String, String> {

    @Override
    public void processElement(ProcessContext ctx) throws Exception {
      for (JsonNode hashtag : jsonMapper.readTree(ctx.element())
                                        .with("entities")
                                        .withArray("hashtags")) {
        ctx.output(hashtag.get("text").asText());
      }
    }
  }

  // return timestamp from "timestamp_ms" field.
  static SerializableFunction<String, Instant> timestampFn = json -> {
    try {
      long timestamp_ms = jsonMapper
          .readTree(json)
          .path("timestamp_ms")
          .asLong();
      return timestamp_ms == 0 ? Instant.now() : new Instant(timestamp_ms);
    } catch (Exception e) {
      throw new RuntimeException("Incorrect json", e);
    }
  };

  public static void main(String args[]) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    final int windowSize = options.getSlidingWindowSize();
    final int windowPeriod = options.getSlidingWindowPeriod();

    UnboundedSource<String, ?> kafkaSource = KafkaSource
        .<String>unboundedValueSourceBuilder()
        .withBootstrapServers(options.getBootstrapServers())
        .withTopics(options.getTopics())
        .withValueDecoderFn(bytes -> (bytes == null) ? null : new String(bytes, Charsets.UTF_8))
        .withTimestampFn(timestampFn)
        .build();

    pipeline
      .apply(Read.from(kafkaSource).named("sample_tweets"))
      .apply(ParDo.of(new ExtractHashtagsFn()))
      .apply(Window.<String>into(SlidingWindows
          .of(Duration.standardMinutes(windowSize))
          .every(Duration.standardSeconds(windowPeriod))))
      .apply(Count.perElement())
      .apply(Top.of(options.getNumTopHashtags(), new KV.OrderByValue<String, Long>()).withoutDefaults())
      .apply(FlatMapElements
          .via((List<KV<String, Long>> top) -> {
            LOG.info("Top Hashtags in {} minutes : {}", windowSize, top);
            return ImmutableList.<Long>of();})
          .withOutputType(new TypeDescriptor<Long>(){}));

    pipeline.run();
  }
}

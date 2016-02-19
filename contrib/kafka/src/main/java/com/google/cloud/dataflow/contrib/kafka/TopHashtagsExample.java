package com.google.cloud.dataflow.contrib.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
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
  }

  public static void main(String args[]) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    UnboundedSource<ConsumerRecord<String, String>, ?> kafkaSource = KafkaSource
        .<String, String>unboundedSourceBuilder()
        .withBootstrapServers("localhost:9092")
        .withTopics(ImmutableList.of("sample_tweets_json"))
        .withKeyDecoderFn(  bytes -> (bytes == null) ? null : new String(bytes, Charsets.UTF_8))
        .withValueDecoderFn(bytes -> (bytes == null) ? null : new String(bytes, Charsets.UTF_8))
        .build();

    pipeline
      .apply(Read.from(kafkaSource).named("sample_tweets"))
      .apply(MapElements
          .<ConsumerRecord<String, String>, Integer>via(r -> 1)
          .withOutputType(new TypeDescriptor<Integer>(){}))
      .apply(Window.<Integer>into(SlidingWindows
          .of(Duration.standardMinutes(options.getSlidingWindowSize()))
          .every(Duration.standardMinutes(options.getSlidingWindowPeriod()))))
      .apply(Count.<Integer>globally().withoutDefaults())
      .apply(FlatMapElements
          .<Long, Long>via(count -> {
            LOG.info("Tweets in last 5 minutes : %d", count);
            return ImmutableList.<Long>of();})
          .withOutputType(new TypeDescriptor<Long>(){}));

    pipeline.run();
  }
}

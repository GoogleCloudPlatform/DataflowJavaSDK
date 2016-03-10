/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.kafka.examples;

import com.google.cloud.dataflow.contrib.kafka.KafkaIO;
import com.google.cloud.dataflow.contrib.kafka.KafkaIO.Read;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation.Required;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Top;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This Dataflow app show cases {@link KafkaIO}. The application reads from a Kafka topic
 * containing <a href="https://dev.twitter.com/overview/api/tweets">JSON Tweets</a>, calculates top
 * hashtags in 10 minute window. The results are written back to a Kafka topic.
 *
 * <pre>{@code
 * Usage:
 *   $ java -cp jar_with_dependencies.jar                                           \
 *          com.google.cloud.dataflow.contrib.kafka.examples.TopHashtagsExample     \
 *          --project=GCP_PROJECT                                                   \
 *          --stagingLocation=GS_STAGING_DIRECTORY                                  \
 *          --runner=BlockingDataflowPipelineRunner                                 \
 *          --bootstrapServers="kafka_server_1:9092"                                \
 *          --topics="sample_tweets_json"                                           \
 *          --outputTopic="top_hashtags"
 * }</pre>
 *
 */
public class TopHashtagsExample {

  private static final Logger LOG = LoggerFactory.getLogger(TopHashtagsExample.class);

  /**
   * Options for the app.
   */
  public static interface Options extends PipelineOptions {
    @Description("Sliding window size, in minutes")
    @Default.Integer(10)
    Integer getSlidingWindowSize();
    void setSlidingWindowSize(Integer value);

    @Description("Trigger window period, in minutes")
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

    @Description("Number of Top Hashtags to track")
    @Default.Integer(10)
    Integer getNumTopHashtags();
    void setNumTopHashtags(Integer count);

    @Description("Kafka topic name for writing results")
    @Required
    String getOutputTopic();
    void setOutputTopic(String topic);
  }

  public static void main(String args[]) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);

    final int windowSize = options.getSlidingWindowSize();
    final int windowPeriod = options.getSlidingWindowPeriod();

    Read<?, String> reader = KafkaIO.read()
        .withBootstrapServers(options.getBootstrapServers())
        .withTopics(options.getTopics())
        .withValueCoder(StringUtf8Coder.of())
        .withTimestampFn(timestampFn);

    pipeline
      .apply("sample_tweets", reader)
      .apply(Values.<String>create())
      .apply(ParDo.of(new ExtractHashtagsFn()))
      .apply(Window.<String>into(SlidingWindows
          .of(Duration.standardMinutes(windowSize))
          .every(Duration.standardMinutes(windowPeriod))))
      .apply(Count.<String>perElement())
      .apply(Top.of(options.getNumTopHashtags(), new KV.OrderByValue<String, Long>())
                .withoutDefaults())
      .apply(ParDo.of(new OutputFormatter()))
      .apply(ParDo.of(new KafkaWriter(options)));

    pipeline.run();
  }

  // The rest of the file implements DoFns to extract hashtags, formatting output, writing output
  // back to Kafka. Note that writing to Kafka is not a complete Dataflow Sink. It is a best-effort
  // logging of the results.

  private static final ObjectMapper jsonMapper = new ObjectMapper();

  /**
   * Emit hashtags in the tweet (if any).
   */
  private static class ExtractHashtagsFn extends DoFn<String, String> {

    @Override
    public void processElement(ProcessContext ctx) throws Exception {
      for (JsonNode hashtag : jsonMapper.readTree(ctx.element())
          .with("entities")
          .withArray("hashtags")) {
        ctx.output(hashtag.get("text").asText());
      }
    }
  }

  // extract timestamp from "timestamp_ms" field.
  private static SerializableFunction<KV<byte[], String>, Instant> timestampFn =
      new SerializableFunction<KV<byte[], String>, Instant>() {
        @Override
        public Instant apply(KV<byte[], String> kv) {
          try {
            long tsMillis = jsonMapper.readTree(kv.getValue()).path("timestamp_ms").asLong();
            return tsMillis == 0 ? Instant.now() : new Instant(tsMillis);
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      };

  // return json string containing top hashtags and window information time
  private static class OutputFormatter extends DoFn<List<KV<String, Long>>, String>
      implements DoFn.RequiresWindowAccess {

    private transient DateTimeFormatter formatter;
    private transient ObjectWriter jsonWriter;

    static class OutputJson {
      @JsonProperty String windowStart;
      @JsonProperty String windowEnd;
      @JsonProperty String generatedAt;
      @JsonProperty List<HashtagInfo> topHashtags;

      OutputJson(String windowStart, String windowEnd,
                 String generatedAt, List<HashtagInfo> topHashtags) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.generatedAt = generatedAt;
        this.topHashtags = topHashtags;
      }
    }

    static class HashtagInfo {
      @JsonProperty final String hashtag;
      @JsonProperty final long count;
      HashtagInfo(String hashtag, long count) {
        this.hashtag = hashtag;
        this.count = count;
      }
    }

    @Override
    public void processElement(ProcessContext ctx) throws Exception {
      if (formatter == null) {
        formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC();
        jsonWriter = new ObjectMapper().writerWithType(OutputJson.class);
      }

      List<HashtagInfo> topHashtags = Lists.newArrayListWithCapacity(ctx.element().size());

      for (KV<String, Long> tag : ctx.element()) {
        topHashtags.add(new HashtagInfo(tag.getKey(), tag.getValue()));
      }

      IntervalWindow window = (IntervalWindow) ctx.window();

      String json = jsonWriter.writeValueAsString(new OutputJson(
          formatter.print(window.start()),
          formatter.print(window.end()),
          formatter.print(Instant.now()),
          topHashtags));

      ctx.output(json);
    }
  }

  private static class KafkaWriter extends DoFn<String, Void> {

    private final String topic;
    private final Map<String, Object> config;
    private transient KafkaProducer<String, String> producer = null;

    public KafkaWriter(Options options) {
      this.topic = options.getOutputTopic();
      this.config = ImmutableMap.<String, Object>of(
          "bootstrap.servers", options.getBootstrapServers(),
          "key.serializer",    StringSerializer.class.getName(),
          "value.serializer",  StringSerializer.class.getName());
    }

    @Override
    public void processElement(ProcessContext ctx) throws Exception {
      if (producer == null) {
        producer = new KafkaProducer<String, String>(config);
      }
      LOG.info("Top Hashtags : " + ctx.element());
      producer.send(new ProducerRecord<String, String>(topic, ctx.element()));
    }
  }
}

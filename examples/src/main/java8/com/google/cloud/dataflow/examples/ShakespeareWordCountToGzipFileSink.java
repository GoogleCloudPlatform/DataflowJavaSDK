/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;

import java.util.Arrays;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.DecoratedFileSink;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.Write;
import com.google.cloud.dataflow.sdk.io.WriterOutputGzipDecoratorFactory;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.FlatMapElements;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Preconditions;

/**
 * Example Google Dataflow pipeline showing how to use the {@link DecoratedFileSink} and
 * {@link WriterOutputGzipDecoratorFactory} classes.
 * <p>
 * This does a word count against the files in {@code gs://dataflow-samples/shakespeare/*} resulting
 * in output records of single {@value <word>,<count>} pairs, with the final output files in both
 * plain text and Gzip compressed format.
 *
 * @author jeffkpayne
 * @see DecoratedFileSink
 * @see WriterOutputGzipDecoratorFactory
 */
public class ShakespeareWordCountToGzipFileSink {
  public static void main(String[] args) {
    // Set any necessary pipeline options.
    final DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setRunner(BlockingDataflowPipelineRunner.class);
    options.setProject("<YOUR_GCP_PROJECT_ID>");
    options.setStagingLocation("<YOUR_GCS_STAGING_LOCATION>");
    // Wire together pipeline transforms.
    final Pipeline p = Pipeline.create(options);
    final PCollection<String> wc = p.apply(TextIO.Read.from("gs://dataflow-samples/shakespeare/*"))
        .apply(FlatMapElements.via((String word) -> Arrays.asList(word.split("[^a-zA-Z']+")))
            .withOutputType(new TypeDescriptor<String>() {}))
        .apply(Filter.byPredicate((String word) -> !word.isEmpty()))
        .apply(Count.<String>perElement())
        .apply(MapElements
            .via((KV<String, Long> wordCount) -> wordCount.getKey() + "," + wordCount.getValue())
            .withOutputType(new TypeDescriptor<String>() {}));
    wc.apply(TextIO.Write.named("WritePlainTextToGCS").to("<YOUR_GCS_PLAIN_TEXT_OUTPUT_LOCATION>"));
    wc.apply("WriteGzipToGCS",
        Write.to(new DecoratedFileSink<String>("<YOUR_GCS_PLAIN_TEXT_OUTPUT_LOCATION>", "txt",
            TextIO.DEFAULT_TEXT_CODER, null, null,
            WriterOutputGzipDecoratorFactory.getInstance())));
    // Run pipeline.
    p.run();
  }
}

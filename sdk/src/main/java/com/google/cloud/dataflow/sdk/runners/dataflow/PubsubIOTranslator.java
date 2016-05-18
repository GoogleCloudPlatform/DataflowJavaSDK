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

package com.google.cloud.dataflow.sdk.runners.dataflow;

import com.google.cloud.dataflow.sdk.io.PubsubUnboundedSink;
import com.google.cloud.dataflow.sdk.io.PubsubUnboundedSource;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TransformTranslator;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineTranslator.TranslationContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.common.base.Preconditions;

/**
 * Pubsub transform support code for the Dataflow backend.
 */
public class PubsubIOTranslator {

  /**
   * Implements PubsubIO Read translation for the Dataflow backend.
   */
  public static class ReadTranslator<T> implements TransformTranslator<PubsubUnboundedSource<T>> {
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void translate(
        PubsubUnboundedSource<T> transform,
        TranslationContext context) {
      translateReadHelper(transform, context);
    }

    private <T> void translateReadHelper(
        PubsubUnboundedSource<T> transform,
        TranslationContext context) {
      Preconditions.checkState(context.getPipelineOptions().isStreaming(),
          "PubsubIOTranslator.ReadTranslator is only for streaming pipelines.");

      context.addStep(transform, "ParallelRead");
      context.addInput(PropertyNames.FORMAT, "pubsub");
      if (transform.getTopic() != null) {
        context.addInput(PropertyNames.PUBSUB_TOPIC, transform.getTopic().getV1Beta1Path());
      }
      if (transform.getSubscription() != null) {
        context.addInput(
            PropertyNames.PUBSUB_SUBSCRIPTION, transform.getSubscription().getV1Beta1Path());
      }
      if (transform.getTimestampLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_TIMESTAMP_LABEL, transform.getTimestampLabel());
      }
      if (transform.getIdLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_ID_LABEL, transform.getIdLabel());
      }
      context.addValueOnlyOutput(PropertyNames.OUTPUT, context.getOutput(transform));
    }
  }

  /**
   * Implements PubsubIO Write translation for the Dataflow backend.
   */
  public static class WriteTranslator<T> implements TransformTranslator<PubsubUnboundedSink<T>> {

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void translate(
        PubsubUnboundedSink<T> transform,
        TranslationContext context) {
      translateWriteHelper(transform, context);
    }

    private <T> void translateWriteHelper(
        PubsubUnboundedSink<T> transform,
        TranslationContext context) {
      Preconditions.checkState(context.getPipelineOptions().isStreaming(),
          "PubsubIOTranslator.WriteTranslator is only for streaming pipelines.");

      context.addStep(transform, "ParallelWrite");
      context.addInput(PropertyNames.FORMAT, "pubsub");
      context.addInput(PropertyNames.PUBSUB_TOPIC, transform.getTopic().getV1Beta1Path());
      if (transform.getTimestampLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_TIMESTAMP_LABEL, transform.getTimestampLabel());
      }
      if (transform.getIdLabel() != null) {
        context.addInput(PropertyNames.PUBSUB_ID_LABEL, transform.getIdLabel());
      }
      context.addEncodingInput(WindowedValue.getValueOnlyCoder(transform.getElementCoder()));
      context.addInput(PropertyNames.PARALLEL_INPUT, context.getInput(transform));
    }
  }
}

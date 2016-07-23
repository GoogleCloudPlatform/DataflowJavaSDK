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

package com.google.cloud.dataflow.sdk.runners;

import com.google.auto.service.AutoService;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineDebugOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PipelineRunner} that's like {@link DataflowPipelineRunner} but only stores a template of
 * a job.
 *
 * <p>Requires that {@link getDataflowJobFile} is set.
 */
public class TemplatingDataflowPipelineRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final Logger LOG = LoggerFactory.getLogger(TemplatingDataflowPipelineRunner.class);

  private final DataflowPipelineRunner dataflowPipelineRunner;

  protected TemplatingDataflowPipelineRunner(
      DataflowPipelineRunner internalRunner) {
    this.dataflowPipelineRunner = internalRunner;
  }

  private static class TemplateHooks extends DataflowPipelineRunnerHooks {
    @Override
    public boolean shouldActuallyRunJob() {
      return false;
    }

    @Override
    public boolean failOnJobFileWriteFailure() {
      return true;
    }
  }

  /** Constructs a runner from the provided options. */
  public static TemplatingDataflowPipelineRunner fromOptions(PipelineOptions options) {
    DataflowPipelineDebugOptions dataflowOptions =
        PipelineOptionsValidator.validate(DataflowPipelineDebugOptions.class, options);
    DataflowPipelineRunner dataflowPipelineRunner =
        DataflowPipelineRunner.fromOptions(dataflowOptions);
    Preconditions.checkNotNull(dataflowOptions.getDataflowJobFile());

    return new TemplatingDataflowPipelineRunner(dataflowPipelineRunner);
  }

  /** {@inheritDoc} */
  @Override
  public DataflowPipelineJob run(Pipeline p) {
    dataflowPipelineRunner.setHooks(new TemplateHooks());
    final DataflowPipelineJob job = dataflowPipelineRunner.run(p);
    LOG.info("Template successfully created.");
    return null;
  }

  @Override
  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    return dataflowPipelineRunner.apply(transform, input);
  }

  @Override
  public String toString() {
    return "TemplatingDataflowPipelineRunner";
  }

  /** Register the {@link TemplatingDataflowPipelineRunner}. */
  @AutoService(PipelineRunnerRegistrar.class)
  public static class Runner implements PipelineRunnerRegistrar {
    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.<Class<? extends PipelineRunner<?>>>of(
          TemplatingDataflowPipelineRunner.class);
    }
  }
}

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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.common.collect.ImmutableMap;

import java.util.List;

/**
 * A factory that dispatches to all known factories in the Dataflow SDK based on the value of
 * {@link CloudObject#getClassName()} for the specified {@code DoFn}.
 */
public class DefaultParDoFnFactory implements ParDoFnFactory {
  private final ImmutableMap<String, ParDoFnFactory> defaultFactories;

  public DefaultParDoFnFactory() {
    defaultFactories = ImmutableMap.<String, ParDoFnFactory>builder()
        .put("DoFn", new NormalParDoFn.Factory())
        .put("CombineValuesFn", new CombineValuesFn.Factory())
        .put("MergeBucketsDoFn", new GroupAlsoByWindowsParDoFn.Factory())
        .put("AssignBucketsDoFn", new AssignWindowsParDoFn.Factory())
        .put("MergeWindowsDoFn", new GroupAlsoByWindowsParDoFn.Factory())
        .put("AssignWindowsDoFn", new AssignWindowsParDoFn.Factory())
        .put("ReifyTimestampAndWindowsDoFn", new ReifyTimestampAndWindowsParDoFn.Factory())
        .build();
  }

  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      String stepName,
      String transformName,
      List<SideInputInfo> sideInputInfos,
      List<MultiOutputInfo> multiOutputInfos,
      int numOutputs,
      DataflowExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler)
          throws Exception {

    String className = cloudUserFn.getClassName();
    ParDoFnFactory factory = defaultFactories.get(className);

    if (factory == null) {
      throw new Exception("No known ParDoFnFactory for " + className);
    }

    return factory.create(
        options,
        cloudUserFn,
        stepName,
        transformName,
        sideInputInfos,
        multiOutputInfos,
        numOutputs,
        executionContext,
        addCounterMutator,
        stateSampler);
  }
}

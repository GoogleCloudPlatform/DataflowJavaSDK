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

package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.TransformTreeNode;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.transforms.display.HasDisplayData;
import com.google.cloud.dataflow.sdk.values.PValue;

/**
 * Validate correct implementation of {@link DisplayData} by evaluating
 * {@link HasDisplayData#populateDisplayData(DisplayData.Builder)} during pipeline construction.
 */
class DisplayDataValidator {
  // Do not instantiate
  private DisplayDataValidator() {}

  static void validatePipeline(Pipeline pipeline) {
    validateOptions(pipeline);
    validateTransforms(pipeline);
  }

  private static void validateOptions(Pipeline pipeline) {
    evaluateDisplayData(pipeline.getOptions());
  }

  private static void validateTransforms(Pipeline pipeline) {
    pipeline.traverseTopologically(Visitor.INSTANCE);
  }

  private static void evaluateDisplayData(HasDisplayData component) {
    DisplayData.from(component);
  }

  private static class Visitor implements Pipeline.PipelineVisitor {
    private static final Visitor INSTANCE = new Visitor();

    @Override
    public void enterCompositeTransform(TransformTreeNode node) {
      if (!node.isRootNode()) {
        evaluateDisplayData(node.getTransform());
      }
    }

    @Override
    public void visitTransform(TransformTreeNode node) {
      evaluateDisplayData(node.getTransform());
    }

    @Override
    public void leaveCompositeTransform(TransformTreeNode node) {}

    @Override
    public void visitValue(PValue value, TransformTreeNode producer) {}
  }
}

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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link StepTransformResult}.
 */
@RunWith(JUnit4.class)
public class StepTransformResultTest {
  private AppliedPTransform<?, ?, ?> transform;
  private BundleFactory bundleFactory;
  private PCollection<Integer> pc;

  @Before
  public void setup() {
    TestPipeline p = TestPipeline.create();
    pc = p.apply(Create.of(1, 2, 3));
    transform = pc.getProducingTransformInternal();

    bundleFactory = InProcessBundleFactory.create();
  }

  @Test
  public void producedBundlesProducedOutputs() {
    InProcessTransformResult result = StepTransformResult.withoutHold(transform)
        .addOutput(bundleFactory.createRootBundle(pc))
        .build();

    assertThat(result.producedOutput(), is(true));
  }

  @Test
  public void withAdditionalOutputProducedOutputs() {
    InProcessTransformResult result =
        StepTransformResult.withoutHold(transform).withAdditionalOutput(true).build();

    assertThat(result.producedOutput(), is(true));
  }

  @Test
  public void producedBundlesAndAdditionalOutputProducedOutputs() {
    InProcessTransformResult result = StepTransformResult.withoutHold(transform)
        .addOutput(bundleFactory.createRootBundle(pc))
        .withAdditionalOutput(true)
        .build();

    assertThat(result.producedOutput(), is(true));
  }

  @Test
  public void noBundlesNoAdditionalOutputProducedOutputsFalse() {
    InProcessTransformResult result = StepTransformResult.withoutHold(transform).build();

    assertThat(result.producedOutput(), is(false));
  }
}

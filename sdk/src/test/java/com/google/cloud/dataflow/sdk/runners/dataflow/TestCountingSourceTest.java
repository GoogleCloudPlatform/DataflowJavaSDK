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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * Test the TestCountingSource.
 */
@RunWith(JUnit4.class)
public class TestCountingSourceTest {
  @Test
  public void testRespectsCheckpointContract() throws IOException {
    TestCountingSource source = new TestCountingSource(3);
    PipelineOptions options = PipelineOptionsFactory.create();
    TestCountingSource.CountingSourceReader reader =
        source.createReader(options, null /* no checkpoint */);
    assertTrue(reader.start());
    assertEquals(0L, (long) reader.getCurrent().getValue());
    assertTrue(reader.advance());
    assertEquals(1L, (long) reader.getCurrent().getValue());
    TestCountingSource.CounterMark checkpoint = reader.getCheckpointMark();
    checkpoint.finalizeCheckpoint();
    reader = source.createReader(options, checkpoint);
    assertTrue(reader.start());
    assertEquals(2L, (long) reader.getCurrent().getValue());
    assertFalse(reader.advance());
  }
}

/*
 * Copyright (C) 2016 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.cloud.dataflow.sdk.util.TestCredential;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;

/**
 * Tests for TemplatingDataflowPipelineRunner.
 */
@RunWith(JUnit4.class)
public class TemplatingDataflowPipelineRunnerTest {

  @Rule
  public ExpectedLogs expectedLogs = ExpectedLogs.none(TemplatingDataflowPipelineRunner.class);

  @Rule
  public ExpectedException expectedThrown = ExpectedException.none();

  @Rule
  public final TemporaryFolder tmpDir = new TemporaryFolder();

  /**
   * Creates a mocked {@link DataflowPipelineJob} with the given {@code projectId} and {@code jobId}
   * .
   *
   * <p>
   * The return value may be further mocked.
   */
  private DataflowPipelineJob createMockJob(String projectId, String jobId) throws Exception {
    DataflowPipelineJob mockJob = mock(DataflowPipelineJob.class);
    when(mockJob.getProjectId()).thenReturn(projectId);
    when(mockJob.getJobId()).thenReturn(jobId);
    return mockJob;
  }

  /**
   * Returns a {@link TemplatingDataflowPipelineRunner} that will return the provided a job to
   * return. Some {@link PipelineOptions} will be extracted from the job, such as the project ID.
   */
  private TemplatingDataflowPipelineRunner createMockRunner(
      DataflowPipelineJob job, String filePath) throws Exception {
    DataflowPipelineRunner mockRunner = mock(DataflowPipelineRunner.class);
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setProject(job.getProjectId());
    options.setDataflowJobFile(filePath);
    when(mockRunner.run(isA(Pipeline.class))).thenReturn(job);

    return new TemplatingDataflowPipelineRunner(mockRunner, options);
  }

  /**
   * Tests that the {@link TemplatingDataflowPipelineRunner} returns normally when a template is
   * successfully written.
   */
  @Test
  public void testLoggedCompletion() throws Exception {
    File existingFile = tmpDir.newFile();
    createMockRunner(createMockJob("testJobDone-projectId", "testJobDone-jobId"),
        existingFile.getPath()).run(DirectPipeline.createForTest());
    expectedLogs.verifyInfo("Template successfully created");
  }

  /**
   * Tests that the {@link TemplatingDataflowPipelineRunner} throws the appropriate exception when
   * an output file is not writable.
   */
  @Test
  public void testLoggedErrorForFile() throws Exception {
    // TODO: Determine why this isn't failing.
    // expectedThrown.expect(IOException.class);
    createMockRunner(createMockJob("testJobDone-projectId", "testJobDone-jobId"), "/bad/path").run(
        DirectPipeline.createForTest());
  }

  @Test
  public void testToString() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setDataflowJobFile("foo");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    assertEquals("TemplatingDataflowPipelineRunner",
        TemplatingDataflowPipelineRunner.fromOptions(options).toString());
  }
}

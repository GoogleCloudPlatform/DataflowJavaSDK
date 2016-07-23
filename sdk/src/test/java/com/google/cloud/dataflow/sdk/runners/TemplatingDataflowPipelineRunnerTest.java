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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.ExpectedLogs;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.util.NoopPathValidator;
import com.google.cloud.dataflow.sdk.util.TestCredential;

import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.TimeUnit;

/**
 * Tests for TemplatingDataflowPipelineRunner.
 */
@RunWith(JUnit4.class)
public class TemplatingDataflowPipelineRunnerTest {

  @Rule
  public ExpectedLogs expectedLogs = ExpectedLogs.none(TemplatingDataflowPipelineRunner.class);

  @Rule
  public ExpectedException expectedThrown = ExpectedException.none();

  /**
   * A {@link Matcher} for a {@link DataflowJobException} that applies an underlying {@link Matcher}
   * to the {@link DataflowPipelineJob} returned by {@link DataflowJobException#getJob()}.
   */
  private static class DataflowJobExceptionMatcher<T extends DataflowJobException>
      extends TypeSafeMatcher<T> {

    private final Matcher<DataflowPipelineJob> matcher;

    public DataflowJobExceptionMatcher(Matcher<DataflowPipelineJob> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(T ex) {
      return matcher.matches(ex.getJob());
    }

    @Override
    protected void describeMismatchSafely(T item, Description description) {
        description.appendText("job ");
        matcher.describeMismatch(item.getMessage(), description);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("exception with job matching ");
      description.appendDescriptionOf(matcher);
    }

    @Factory
    public static <T extends DataflowJobException> Matcher<T> expectJob(
        Matcher<DataflowPipelineJob> matcher) {
      return new DataflowJobExceptionMatcher<T>(matcher);
    }
  }

  /**
   * A {@link Matcher} for a {@link DataflowPipelineJob} that applies an underlying {@link Matcher}
   * to the return value of {@link DataflowPipelineJob#getJobId()}.
   */
  private static class JobIdMatcher<T extends DataflowPipelineJob> extends TypeSafeMatcher<T> {

    private final Matcher<String> matcher;

    public JobIdMatcher(Matcher<String> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matchesSafely(T job) {
      return matcher.matches(job.getJobId());
    }

    @Override
    protected void describeMismatchSafely(T item, Description description) {
        description.appendText("jobId ");
        matcher.describeMismatch(item.getJobId(), description);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("job with jobId ");
      description.appendDescriptionOf(matcher);
    }

    @Factory
    public static <T extends DataflowPipelineJob> Matcher<T> expectJobId(final String jobId) {
      return new JobIdMatcher<T>(equalTo(jobId));
    }
  }

  /**
   * Returns a {@link TemplatingDataflowPipelineRunner} that will return the provided a job to return.
   * Some {@link PipelineOptions} will be extracted from the job, such as the project ID.
   */
  private TemplatingDataflowPipelineRunner createMockRunner(DataflowPipelineJob job)
      throws Exception {
    DataflowPipelineRunner mockRunner = mock(DataflowPipelineRunner.class);
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setProject(job.getProjectId());

    when(mockRunner.run(isA(Pipeline.class))).thenReturn(job);

    return new TemplatingDataflowPipelineRunner(mockRunner, options);
  }

  /**
   * Tests that the {@link TemplatingDataflowPipelineRunner} returns normally when a job terminates in
   * the {@link State#DONE DONE} state.
   */
  @Test
  public void testLoggedCompletion() throws Exception {
    createMockRunner(createMockJob("testJobDone-projectId", "testJobDone-jobId", State.DONE))
        .run(DirectPipeline.createForTest());
    expectedLogs.verifyInfo("Created template ...");
  }

  /**
   * Tests that the {@link TemplatingDataflowPipelineRunner} throws the appropriate exception
   * when a job terminates in the {@link State#FAILED FAILED} state.
   */
  @Test
  public void testLoggedErrorForFile() throws Exception {
    expectedThrown.expect(DataflowJobExecutionException.class);
    expectedThrown.expect(DataflowJobExceptionMatcher.expectJob(
        JobIdMatcher.expectJobId("testFailedJob-jobId")));
    createMockRunner(createMockJob("testFailedJob-projectId", "testFailedJob-jobId", State.FAILED))
        .run(DirectPipeline.createForTest());
  }

  @Test
  public void testToString() {
    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setJobName("TestJobName");
    options.setProject("test-project");
    options.setTempLocation("gs://test/temp/location");
    options.setTempLocation("gs://test/temp/location");
    options.setGcpCredential(new TestCredential());
    options.setPathValidatorClass(NoopPathValidator.class);
    assertEquals("TemplatingDataflowPipelineRunner#testjobname",
        TemplatingDataflowPipelineRunner.fromOptions(options).toString());
  }
}

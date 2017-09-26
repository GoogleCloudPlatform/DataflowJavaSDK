/*
 * Copyright (C) 2017 Google Inc.
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
package org.apache.beam.runners.dataflow;

import static org.junit.Assert.assertEquals;

import java.io.InputStream;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DataflowRunnerInfo} specifically validating that properties in
 * this distrbution are correctly read.
 */
@RunWith(JUnit4.class)
public class DataflowRunnerInfoOverrideTest {
  private static final String DATAFLOW_DISTRIBUTION_PROPERTIES_PATH =
      "/org/apache/beam/runners/dataflow/dataflow-distribution.properties";

  private static final String FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY =
      "fnapi.environment.major.version";
  private static final String LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY =
      "legacy.environment.major.version";
  private static final String CONTAINER_VERSION_KEY = "container.version";


  @Test
  public void testDataflowDistributionOverride() throws Exception {
    try (InputStream in =
        DataflowRunnerInfo.class.getResourceAsStream(DATAFLOW_DISTRIBUTION_PROPERTIES_PATH)) {
      Properties properties = new Properties();
      properties.load(in);

      assertEquals(properties.getProperty(FNAPI_ENVIRONMENT_MAJOR_VERSION_KEY),
          DataflowRunnerInfo.getDataflowRunnerInfo().getFnApiEnvironmentMajorVersion());
      assertEquals(properties.getProperty(LEGACY_ENVIRONMENT_MAJOR_VERSION_KEY),
          DataflowRunnerInfo.getDataflowRunnerInfo().getLegacyEnvironmentMajorVersion());
      assertEquals(properties.getProperty(CONTAINER_VERSION_KEY),
          DataflowRunnerInfo.getDataflowRunnerInfo().getContainerVersion());
    }
  }
}

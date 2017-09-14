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
    try (InputStream in
        = DataflowRunnerInfo.class.getResourceAsStream(DATAFLOW_DISTRIBUTION_PROPERTIES_PATH)) {
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

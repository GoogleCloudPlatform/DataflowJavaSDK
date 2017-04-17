package com.google.wave.prototype.dataflow.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.wave.prototype.dataflow.model.SFConfig;
import com.google.wave.prototype.dataflow.util.SFConstants;

/**
 * Unit test for SFConfig
 * Reads the config file present in local and assert the values
 */
public class SFConfigTest {
    @Test
    public void validLocalFile() throws Exception {
        // Config files are present in project home
        StringBuilder sb = new StringBuilder();
        sb.append(SFConstants.LOCAL_FILE_PREFIX);
        sb.append(System.getProperty("user.dir"));
        sb.append("/test_sf_config.json");

        // This will read the config file and populate SFConfig with userId and password
        SFConfig sfConfig = SFConfig.getInstance(sb.toString(), PipelineOptionsFactory.create());

        assertEquals("demo@demo.com", sfConfig.getUserId());
        assertEquals("test", sfConfig.getPassword());
    }

    @Test
    public void invalidLocalFile() throws Exception {
        try {
            // Providing invalid file path which should throw Exception
            SFConfig.getInstance("test_sf_config.json", PipelineOptionsFactory.create());
            fail("Expected exception not raised");
        } catch (Exception e) {
            // Expected exception here
        }
    }
}

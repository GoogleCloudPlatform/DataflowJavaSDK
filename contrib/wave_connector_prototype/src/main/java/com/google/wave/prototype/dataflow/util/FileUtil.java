package com.google.wave.prototype.dataflow.util;

import java.io.File;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Simple Utility to read to the contents from file
 * File can be present in GCS or from local file system
 */
public class FileUtil {

    public static String getContent(String fileLocation, PipelineOptions options) throws Exception {
        // Have separate reader for GS files and local files
        if (fileLocation.startsWith(SFConstants.GS_FILE_PREFIX)) {
            return readFromGCS(fileLocation, options);
        } else {
            return readFromLocal(fileLocation);
        }
    }

    private static String readFromLocal(String configFileLocation) throws Exception {
        // Removing file:// prefix
        String fileLocation = StringUtils.substringAfter(configFileLocation, SFConstants.LOCAL_FILE_PREFIX);
        // Using commons-io utility to read the file as String
        return FileUtils.readFileToString(new File(fileLocation), Charsets.UTF_8);
    }

    private static String readFromGCS(String configFileLocation,
            PipelineOptions options) throws Exception {
        GCSFileUtil gcsFileUtil = new GCSFileUtil(options);
        byte[] contents = gcsFileUtil.read(configFileLocation);
        return new String(contents);
    }
}

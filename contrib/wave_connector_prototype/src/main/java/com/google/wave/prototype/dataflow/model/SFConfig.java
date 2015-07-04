package com.google.wave.prototype.dataflow.model;

import java.io.File;
import java.io.Serializable;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.appengine.repackaged.com.google.gson.Gson;
import com.google.appengine.repackaged.com.google.gson.GsonBuilder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.wave.prototype.dataflow.util.GCSFileUtil;
import com.google.wave.prototype.dataflow.util.SFConstants;

/**
 * Holds the configuration which will be used by SFSource
 * Fetches Salesforce user credentials by reading the configuration file present in GS or local
 * A config file will have the below content
 *				{
 *                	"userId": <salesforce_acccount_id>,
 *                  "password": <salesforce_account_password>
 *   			}
 */
@DefaultCoder(SerializableCoder.class)
public class SFConfig implements Serializable {
    private static final long serialVersionUID = -5569745252294105529L;

    private String userId;
    private String password;

    public static SFConfig getInstance(String configFileLocation, PipelineOptions options) throws Exception {
        validate(configFileLocation);

        String json = getContent(configFileLocation, options);
        Gson gson = new GsonBuilder().create();
        // Unmarshalling file content into SFConfig
        return gson.fromJson(json, SFConfig.class);
    }

    public String getUserId() {
        return userId;
    }

    public String getPassword() {
        return password;
    }

    private static String getContent(String configFileLocation, PipelineOptions options) throws Exception {
        // Have separate reader for GS files and local files
        if (configFileLocation.startsWith(SFConstants.GS_FILE_PREFIX)) {
            return readFromGCS(configFileLocation, options);
        } else {
            return readFromLocal(configFileLocation);
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
        // Content will be in JSON
        // So returning as String which will constructed into this SFConfig bean using GSON
        return new String(contents);
    }

    private static void validate(String configFileLocation) throws Exception {
        // Checking whether the file is provided in proper format
        // GS file should start with gs://
        // local file should start with file://
        if (!StringUtils.isEmpty(configFileLocation)) {
            if (configFileLocation.startsWith(SFConstants.GS_FILE_PREFIX) ||
                    configFileLocation.startsWith(SFConstants.LOCAL_FILE_PREFIX)) {
                return;
            }
        }

        // Provided configFileLocation is not valid
        // Stopping the Job
        throw new Exception("Invalid Configuration file " + configFileLocation);
    }

}

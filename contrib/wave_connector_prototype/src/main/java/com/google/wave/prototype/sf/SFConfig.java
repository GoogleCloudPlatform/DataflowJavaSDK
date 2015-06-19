package com.google.wave.prototype.sf;

import java.io.IOException;
import java.io.Serializable;

import com.google.appengine.repackaged.com.google.gson.Gson;
import com.google.appengine.repackaged.com.google.gson.GsonBuilder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.wave.prototype.google.GCSFileUtil;

/**
 * Holds the configuration which will be used by SFSource
 * Fetches Salesforce user credentials by reading the configuration file present in GCS
 *
 */
@DefaultCoder(SerializableCoder.class)
public class SFConfig implements Serializable {
    private static final long serialVersionUID = -5569745252294105529L;

    private String userId;
    private String password;

    public static SFConfig getInstance(String configFileLocation, PipelineOptions options) throws IOException {
        validate(configFileLocation);
        String json = getContent(configFileLocation, options);
        Gson gson = new GsonBuilder().create();
        return gson.fromJson(json, SFConfig.class);
    }

    public String getUserId() {
        return userId;
    }

    public String getPassword() {
        return password;
    }

    private static String getContent(String configFileLocation, PipelineOptions options) throws IOException {
        GCSFileUtil gcsFileUtil = new GCSFileUtil(options);
        byte[] contents = gcsFileUtil.read(configFileLocation);
        return new String(contents);
    }

    private static void validate(String configFileLocation) {
        if (configFileLocation != null && !configFileLocation.isEmpty()) {
            if (configFileLocation.startsWith("gs://")) {
                if (configFileLocation.indexOf('/') != -1) {
                    return;
                }
            }
        }

        throw new RuntimeException("Invalid Configuration file " + configFileLocation);
    }

}

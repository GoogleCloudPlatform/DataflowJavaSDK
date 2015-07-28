package com.google.wave.prototype.dataflow.model;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.appengine.repackaged.com.google.gson.Gson;
import com.google.appengine.repackaged.com.google.gson.GsonBuilder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.wave.prototype.dataflow.util.FileUtil;
import com.google.wave.prototype.dataflow.util.SFConstants;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

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

    private static final Logger LOG = LoggerFactory.getLogger(SFConfig.class);

    private String userId;
    private String password;

    public static SFConfig getInstance(String configFileLocation, PipelineOptions options) throws Exception {
        validate(configFileLocation);
        // Content will be in JSON
        // So constructing SFConfig bean using GSON
        String json = FileUtil.getContent(configFileLocation, options);
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

    public PartnerConnection createPartnerConnection() throws Exception {
        ConnectorConfig config = new ConnectorConfig();
        LOG.debug("Connecting SF Partner Connection using " + getUserId());
        config.setUsername(getUserId());
        config.setPassword(getPassword());

        try {
            return Connector.newConnection(config);
        } catch (ConnectionException ce) {
            LOG.error("Exception while creating connection", ce);
            throw new Exception(ce);
        }
    }

    public EnterpriseConnection createEnterpriseConnection() throws Exception {
        ConnectorConfig config = new ConnectorConfig();
        LOG.debug("Connecting SF Partner Connection using " + getUserId());
        config.setUsername(getUserId());
        config.setPassword(getPassword());

        try {
            return com.sforce.soap.enterprise.Connector.newConnection(config);
        } catch (ConnectionException ce) {
            LOG.error("Exception while creating connection", ce);
            throw new Exception(ce);
        }
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

package com.google.wave.prototype.dataflow.model;

import java.io.Serializable;

import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;

/**
 * POJO containing,
 * 		SOQL Query to be executed
 * 		{@link SFConfig} to be used to execute SOQL Query
 */
@DefaultCoder(SerializableCoder.class)
public class SFQuery implements Serializable {
    private static final long serialVersionUID = -8477651557035062006L;

    private String query;
    private SFConfig sfConfig;

    public SFQuery(String query, SFConfig sfConfig) {
        this.query = query;
        this.sfConfig = sfConfig;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public SFConfig getSFConfig() {
        return sfConfig;
    }

    public void setSFConfig(SFConfig sfConfig) {
        this.sfConfig = sfConfig;
    }
}

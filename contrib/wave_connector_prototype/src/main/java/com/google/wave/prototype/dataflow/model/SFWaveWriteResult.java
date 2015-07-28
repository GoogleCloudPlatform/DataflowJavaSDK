package com.google.wave.prototype.dataflow.model;

import java.io.Serializable;

/**
 * WriteResult class
 * This just holds the Salesforce object Id of the persisted data
 */
public class SFWaveWriteResult implements Serializable {
    private static final long serialVersionUID = -7451739773848100070L;

    private String sfObjId;

    public SFWaveWriteResult(String sfObjId) {
        this.sfObjId = sfObjId;
    }

    public String getSfObjId() {
        return sfObjId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((sfObjId == null) ? 0 : sfObjId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SFWaveWriteResult other = (SFWaveWriteResult) obj;
        if (sfObjId == null) {
            if (other.sfObjId != null)
                return false;
        } else if (!sfObjId.equals(other.sfObjId))
            return false;
        return true;
    }


}
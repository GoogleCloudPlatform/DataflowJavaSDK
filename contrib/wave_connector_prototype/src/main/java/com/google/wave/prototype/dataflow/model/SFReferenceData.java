package com.google.wave.prototype.dataflow.model;

import java.io.Serializable;

/**
 * POJO containing Salesforce reference data
 */
public class SFReferenceData implements Serializable {
    private static final long serialVersionUID = -7597520654419284165L;

    private String accountId;
    private String opportunityId;
    private String proposalId;

    public SFReferenceData(String accountId, String opportunityId,
            String proposalId) {
        super();
        this.accountId = accountId;
        this.opportunityId = opportunityId;
        this.proposalId = proposalId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getOpportunityId() {
        return opportunityId;
    }

    public void setOpportunityId(String opportunityId) {
        this.opportunityId = opportunityId;
    }

    public String getProposalId() {
        return proposalId;
    }

    public void setProposalId(String proposalId) {
        this.proposalId = proposalId;
    }

    @Override
    public String toString() {
        return accountId + ","+ opportunityId + "," + proposalId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((accountId == null) ? 0 : accountId.hashCode());
        result = prime * result
                + ((opportunityId == null) ? 0 : opportunityId.hashCode());
        result = prime * result
                + ((proposalId == null) ? 0 : proposalId.hashCode());
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
        SFReferenceData other = (SFReferenceData) obj;
        if (accountId == null) {
            if (other.accountId != null)
                return false;
        } else if (!accountId.equals(other.accountId))
            return false;
        if (opportunityId == null) {
            if (other.opportunityId != null)
                return false;
        } else if (!opportunityId.equals(other.opportunityId))
            return false;
        if (proposalId == null) {
            if (other.proposalId != null)
                return false;
        } else if (!proposalId.equals(other.proposalId))
            return false;
        return true;
    }

}

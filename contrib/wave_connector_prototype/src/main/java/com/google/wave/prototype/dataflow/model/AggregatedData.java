package com.google.wave.prototype.dataflow.model;

import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.wave.prototype.dataflow.coder.AggregateDataCoder;

/**
 * POJO holding enriched Salesforce wave data
 * ProposalId, OpportunityId, ClickCount and ImpressionCount
 */
@DefaultCoder(AggregateDataCoder.class)
public class AggregatedData {
    private String proposalId = "";
    private String opportunityId = "";
    private int clickCount = 0;
    private int impressionCount = 0;

    // Used before adding OpportunityId
    public AggregatedData(String proposalId, int clickCount,
            int impressionCount) {
        this.proposalId = proposalId;
        this.clickCount = clickCount;
        this.impressionCount = impressionCount;
    }

    public AggregatedData(String proposalId, String opportunityId, int clickCount,
            int impressionCount) {
        this.proposalId = proposalId;
        this.opportunityId = opportunityId;
        this.clickCount = clickCount;
        this.impressionCount = impressionCount;
    }

    public String getProposalId() {
        return proposalId;
    }

    public void setProposalId(String proposalId) {
        this.proposalId = proposalId;
    }

    public int getClickCount() {
        return clickCount;
    }

    public void setClickCount(int clicksCount) {
        this.clickCount = clicksCount;
    }

    public int getImpressionCount() {
        return impressionCount;
    }

    public void setImpressionCount(int impressionCount) {
        this.impressionCount = impressionCount;
    }

    public void incrementImpressionCount() {
        this.impressionCount++;
    }

    public void incrementClickCount() {
        this.clickCount++;
    }

    public void addImpressionCount(int impressionCount) {
        this.impressionCount += impressionCount;
    }

    public void addClickCount(int clickCount) {
        this.clickCount++;
    }

    public String getOpportunityId() {
        return opportunityId;
    }

    public void setOpportunityId(String opportunityId) {
        this.opportunityId = opportunityId;
    }

    @Override
    public String toString() {
        // Constructs CSV row using fields
        return proposalId + "," + opportunityId + "," + clickCount + "," + impressionCount;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + clickCount;
        result = prime * result + impressionCount;
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
        AggregatedData other = (AggregatedData) obj;
        if (clickCount != other.clickCount)
            return false;
        if (impressionCount != other.impressionCount)
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

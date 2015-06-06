package com.google.wave.prototype.google;

import com.google.api.services.bigquery.model.TableRow;

public class AggregatedData {
	private String proposalId = "";
	private String opportunityId = "";
	private int clickCount = 0;
	private int impressionCount = 0;

	public AggregatedData() {
	}
	
	public AggregatedData(String proposalId, int clickCount,
			int impressionCount) {
		super();
		this.proposalId = proposalId;
		this.clickCount = clickCount;
		this.impressionCount = impressionCount;
	}

	public AggregatedData(String proposalId, String opportunityId, int clickCount,
			int impressionCount) {
		super();
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

	public TableRow asTableRow() {
		TableRow row = new TableRow();
		row.set("proposalId", getProposalId());
		row.set("opportunityId", getOpportunityId());
		row.set("clicks", getClickCount());
		row.set("impressions", getImpressionCount());
		
		return row;
	}

	@Override
	public String toString() {
		return proposalId + "," + opportunityId + "," + clickCount + "," + impressionCount;
	}
	
}

package com.google.wave.prototype.sf;

public class SFReferenceData {
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

	
}

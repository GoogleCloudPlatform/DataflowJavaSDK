package com.google.wave.prototype.sf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.joda.time.Instant;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Source;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.sobject.Opportunity;
import com.sforce.soap.enterprise.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class SFSource extends Source<SFReferenceData> {
	private static final long serialVersionUID = 6057206847961817098L;
	private String sfUserId;
	private String sfPassword;
	
	public static Read.Bound<SFReferenceData> readFrom(String sfUserId, String sfPassword) {
		return Read.from(new SFSource(sfUserId, sfPassword));
	}
	
	public SFSource(String sfUserId, String sfPassword) {
		super();
		this.sfUserId = sfUserId;
		this.sfPassword = sfPassword;
	}

	public String getSfUserId() {
		return sfUserId;
	}

	public String getSfPassword() {
		return sfPassword;
	}
	
	@Override
	public List<? extends Source<SFReferenceData>> splitIntoBundles(
			long desiredBundleSizeBytes, PipelineOptions options)
			throws Exception {
		// TODO - Extend this logic to split the bundles according to SF result
		SFSource sfSource = new SFSource(getSfUserId(), getSfPassword());
		List<Source<SFReferenceData>> splits = new ArrayList<Source<SFReferenceData>>();
		splits.add(sfSource);
		
		return splits;
	}

	@Override
	public void validate() {
		//no op
	}

	public Reader<SFReferenceData> createReader(PipelineOptions options,
			@Nullable ExecutionContext executionContext) throws IOException {
		return new SFReader(this);
	}

	@Override
	public Coder<SFReferenceData> getDefaultOutputCoder() {
		return SFCoder.getInstance();
	}
	
	public static class SFReader implements Source.Reader<SFReferenceData> {
		private SFSource source;
		private EnterpriseConnection connection;
		private SObject[] records;
		private SFReferenceData current;
		private int index = 0;
		private int size = 0;
		
		public SFReader(SFSource source) throws IOException {
			try {
				ConnectorConfig config = new ConnectorConfig();
				config.setUsername(source.getSfUserId());
				config.setPassword(source.getSfPassword());
				
				connection = Connector.newConnection(config);
			} catch (ConnectionException ce) {
				ce.printStackTrace();
				throw new IOException(ce);
			}
		}
		
		@Override
		public boolean start() throws IOException {
			try {
				String query = "SELECT AccountId, Id, proposproposalIdalId__c FROM Opportunity where proposproposalIdalId__c != null";
				QueryResult results = connection.query(query);
				size = results.getSize();
				if (size > index) {
					records = results.getRecords();
					current = constructSFRefData(records[index++]);
				} else {
					return false;
				}
			} catch (ConnectionException ce) {
				ce.printStackTrace();
				throw new IOException(ce);
			}
			
			return true;
		}

		@Override
		public boolean advance() throws IOException {
			if (size > index) {
				current = constructSFRefData(records[index++]);
				return true;
			}
			
			return false;
		}

		@Override
		public SFReferenceData getCurrent() throws NoSuchElementException {
			return current;
		}

		@Override
		public Instant getCurrentTimestamp() throws NoSuchElementException {
			return Instant.now();
		}

		@Override
		public void close() throws IOException {
			try {
				connection.logout();
			} catch (ConnectionException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}

		@Override
		public Source<SFReferenceData> getCurrentSource() {
			return source;
		}

		private SFReferenceData constructSFRefData(SObject sObject) {
			Opportunity opp = (Opportunity) sObject;
			return new SFReferenceData(opp.getAccountId(), opp.getId(), String.valueOf(opp.getProposproposalIdalId__c()));
		}
	}

}

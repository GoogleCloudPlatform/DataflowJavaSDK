/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.contrib.natsio;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.nats.Connection;
import org.nats.MsgHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;

/**
 * <p>{@link NatsIO} provides an API to publish messages to and subscribe subjects on <A href="http://nats.io">NATS server</A>.
 * 
 * <p>NATS server is a high-performance and lightweight cloud native messaging system. A message is a simple string and can be exchanged between
 * clients written in various programming languages.
 * 
 * <p>NatsIO API requires <A href="http://nats.io">NATS server</A> running and accessible from Dataflow workers prior to job submissions. Typically,
 * NATS server is expected to run on <A href="https://cloud.google.com/compute/">Google Compute Engine</A>.
 * 
 * <p>To receive updates from NATS server, use {@link NatsIO.Read.NatsSource} for both batch and streaming purposes. {@link NatsIO.Read#withMaxNumRecords}
 * and {@link NatsIO.Read#withMaxReadtime} can be used as bounded and {@link NatsIO.Read#from} for unbounded source.
 * 
 * <p>For example :
 * <pre> {@code
 *		props.setProperty("servers", "nats://server1:4222");
 *		props.setProperty("queue", "myqueue");
 *
 * 		Pipeline p = Pipeline.create(options);
 *		p
 *			.apply(NatsIO.Read.withMaxNumRecords(subject, maxRecords, props)) // Batch with max records
 *			.apply(ParDo.of(new NatsIOBench.SimpleFunc()));
 * } </pre>
 * 
 * <p>or
 * <pre> {@code
 * 		options.setStreaming(true);
 * 
 * 		Pipeline p = Pipeline.create(options);
 *		p
 *			.apply(NatsIO.Read.from(subject, props)) // Streaming
 *			.apply(ParDo.of(new NatsIOBench.SimpleFunc()));
 * } </pre>
 * 
 */
public class NatsIO {
	private static final Logger LOG = LoggerFactory.getLogger(NatsIO.class);
	
	private static final Coder<String> DEFAULT_CODER = StringUtf8Coder.of();
	private static CommitMark DEFAULT_MARK = new CommitMark();
	private static final byte[] DEFAULT_ID = new byte[1];

	public static class Read {
		/**
		 * Create a unbounded NatsSource instance. 
		 * @param subject to subscribe and receive updates
		 * @param props contains NATS related properties. "servers" property is mandatory to access a remote NATS server.
		 * @return a unbounded NatsSource instance
		 */
		public static com.google.cloud.dataflow.sdk.io.Read.Unbounded<KV<String,String>> from(String subject, Properties props) {
			return com.google.cloud.dataflow.sdk.io.Read.from(new NatsSource(subject, props));
		}

		/**
		 * Create a NatsSource instance which is bounded to a number of updates to receive. 
		 * @param subject to subscribe and received updates
		 * @param numRecords bounds a number of records to receive from NATS server
		 * @param props contains NATS related properties. "servers" property is mandatory to access a remote NATS server.
		 * @return a bounded NatsSource instance
		 */
		public static PTransform<PInput, PCollection<KV<String,String>>> withMaxNumRecords(String subject, long numRecords, Properties props) {
			return from(subject, props).withMaxNumRecords(numRecords);
		}		

		/**
		 * Create a NatsSource instance which is bounded to total reading time to receive updates.
		 * @param subject to subscribe and received updates
		 * @param readTime bounds total reading time to receive updates from NATS server.
		 * @param props contains NATS related properties. "servers" property is mandatory to access a remote NATS server.
		 * @return a bounded NatsSource instance
		 */
		public static PTransform<PInput, PCollection<KV<String,String>>> withMaxReadtime(String subject, long readTime, Properties props) {
			return from(subject, props).withMaxReadTime(Duration.standardSeconds(readTime));
		}		
		
		public static class NatsSource extends UnboundedSource<KV<String,String>, UnboundedSource.CheckpointMark> {
			private static final long serialVersionUID = 1L;
			
			private Properties props = null;
			private Connection conn = null;
			private String subject;
			
			public NatsSource(String subject, Properties props) {
				this.subject = subject;
				this.props = props;
			}
			
			@Override
			public NatsReader createReader(PipelineOptions arg0, UnboundedSource.CheckpointMark arg1) {
				return new NatsReader(subject);
			}

			@Override
			public List<? extends UnboundedSource<KV<String,String>, UnboundedSource.CheckpointMark>> generateInitialSplits(
					int arg0, PipelineOptions arg1) throws Exception {
				List<NatsSource> results = new ArrayList<NatsSource>();
				// Create a NatsSource instance for each split.
				int split = arg0 / 3;
				for(int i = 0; i < split; i++)
					results.add(new NatsSource(subject, props));
				return results;
			}

			@Override
			public Coder<UnboundedSource.CheckpointMark> getCheckpointMarkCoder() {
				return null;
			}

			@Override
			public Coder<KV<String,String>> getDefaultOutputCoder() {
				return KvCoder.of(NatsIO.DEFAULT_CODER, NatsIO.DEFAULT_CODER);
			}

			@Override
			public void validate() {
				Preconditions.checkNotNull(props.getProperty("servers"), "need to set the 'servers' property");
				Preconditions.checkNotNull(subject, "need to set the 'subject'");
			}

			public static class CommitMark implements UnboundedSource.CheckpointMark, Serializable {
				private static final long serialVersionUID = 2L;

				public void finalizeCheckpoint() throws IOException {}		
			}
				
			private class NatsReader extends UnboundedReader<KV<String,String>> {
				private String subject;
				private Integer sid;
				private ConcurrentLinkedQueue<KV<String,String>> queue;
				private KV<String,String> current;
				
				public NatsReader(String subject) {
					this.subject = subject;
					queue = new ConcurrentLinkedQueue<KV<String,String>>();
				}
				
				@Override
				public boolean advance() throws IOException {
					// Polling a queue to check whether an update is available in the queue.
					current = queue.poll();
					if (current == null)
						return false;
					
					return true;
				}

				@Override
				public UnboundedSource.CheckpointMark getCheckpointMark() {
					return DEFAULT_MARK;
				}

				@Override
				public byte[] getCurrentRecordId() throws NoSuchElementException {
					return DEFAULT_ID;
				}

				@Override
				public NatsSource getCurrentSource() {
					return NatsSource.this;
				}

				@Override
				public Instant getWatermark() {
					return Instant.now();
				}

				@Override
				public boolean start() throws IOException {
					if (conn == null) {
						try {
							conn = Connection.connect(props);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					// Subscribe to a subject and attach an event handler to store updates in a queue.
					sid = conn.subscribe(subject, props, new MsgHandler() {
						@Override
						public void execute(String msg, String reply, String subject) {
							queue.add(KV.of(subject, msg));
						}
					});
					return advance();
				}

				@Override
				public void close() throws IOException {
					conn.unsubscribe(sid);
				}

				@Override
				public KV<String,String> getCurrent() throws NoSuchElementException {
					return current;
				}

				@Override
				public Instant getCurrentTimestamp() throws NoSuchElementException {
					return Instant.now();
				}
			}

		}
	}
	
	public static class Write {
		/**
		 * Create a bounded NatsSink instance.
		 * @param props contains NATS related properties. "servers" property is mandatory to access a remote NATS server.
		 * @return a bounded NatsSink instance
		 */
		public static com.google.cloud.dataflow.sdk.transforms.Write.Bound<KV<String,String>> to(Properties props) {
			return  com.google.cloud.dataflow.sdk.transforms.Write.to(new NatsSink(props));			
		}
		
		public static class NatsSink extends Sink<KV<String,String>> {
			private static final long serialVersionUID = -8196259241022418893L;
			
			private Properties props = null;

			public NatsSink(Properties props) {
				this.props = props;
			}
			
			@Override
			public void validate(PipelineOptions options) {
				Preconditions.checkNotNull(props.getProperty("servers"), "need to set the 'servers' property");
			}

			@Override
			public WriteOperation<KV<String,String>, ?> createWriteOperation(PipelineOptions options) {
				return new NatsWriteOperation();
			}
			
			public class NatsWriteOperation extends WriteOperation<KV<String,String>, NatsWriteResult> {
				private static final long serialVersionUID = 8235348290362014433L;

				@Override
				public void initialize(PipelineOptions options) throws Exception {}

			    @Override
			    public Coder<NatsWriteResult> getWriterResultCoder() {
			      return SerializableCoder.of(NatsWriteResult.class);
			    }
			    
				@Override
				public void finalize(Iterable<NatsWriteResult> writerResults, PipelineOptions options) throws Exception {
					long totalEntities = 0;
					for(NatsWriteResult result : writerResults) {
						totalEntities += result.numEntries;
					}
					LOG.debug("Published {} elements.", totalEntities);
				}

				@Override
				public Writer<KV<String, String>, NatsWriteResult> createWriter(
						PipelineOptions options) throws Exception {
					return new NatsWriter(this);
				}

				@Override
				public Sink<KV<String, String>> getSink() {
					return NatsSink.this;
				}
				
			}
			
			public class NatsWriter extends Writer<KV<String,String>, NatsWriteResult> {
				private Connection conn = null;
				private NatsWriteOperation op;
				private long counter = 0;
				
				public NatsWriter(NatsWriteOperation op) {
					this.op = op;
				}
				
				@Override
				public void open(String uId) throws Exception {
						conn = Connection.connect(props);
				}

				/**
				 * Take KV<String, String> as an input and publish it to NATS server.				
				 */
				@Override
				public void write(KV<String, String> entry) throws Exception {
					conn.publish(entry.getKey(), entry.getValue());
					counter++;
				}
				
				@Override
				public NatsWriteResult close() throws Exception {
					return new NatsWriteResult(counter);
				}

				@Override
				public WriteOperation<KV<String, String>, NatsWriteResult> getWriteOperation() {
					return op;
				}

			}

			public class NatsWriteResult implements Serializable {
				private static final long serialVersionUID = -3029995127268645598L;

				private long numEntries;
				
				public NatsWriteResult(long numEntries) {
					this.numEntries = numEntries;
				}
			}
		}
	}
}

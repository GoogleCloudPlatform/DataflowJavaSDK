package com.google.cloud.dataflow.contrib.natsio.example;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.junit.Test;
import org.nats.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.contrib.natsio.NatsIO;
import com.google.cloud.dataflow.contrib.natsio.NatsIOTest;
import com.google.cloud.dataflow.contrib.natsio.NatsIO.Read;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Partition;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import edu.emory.mathcs.backport.java.util.Arrays;

public class NatsIOBench {
	private static final Logger LOG = LoggerFactory.getLogger(NatsIOTest.class);	
	
	static private String servers;
	static private String queue;
	static private int maxRecords;
	static private int maxReadtime;
	static private int loop;
	static private int interval;
	static private int consumers;
	static private int producers;
	static private List<String> subjects;
	static private String subject;
	
	public NatsIOBench() {
		maxRecords = (System.getProperty("nats.maxRecords") == null ? 100 : Integer.parseInt(System.getProperty("nats.maxRecords")));
		maxReadtime = (System.getProperty("nats.maxReadtime") == null ? 100 : Integer.parseInt(System.getProperty("nats.maxReadtime")));

		servers = System.getProperty("nats.servers");
		queue = System.getProperty("nats.queue");
		subjects = Arrays.asList(System.getProperty("subjects").split(","));
		subject =  subjects.get(0);
		
		loop = Integer.parseInt(System.getProperty("loop"));
		interval = Integer.parseInt(System.getProperty("interval"));
		consumers = Integer.parseInt(System.getProperty("consumers"));
		producers = Integer.parseInt(System.getProperty("producers"));
	}
	
	private static class Producer implements Partition.PartitionFn<String>, Serializable {
		private String servers;
		private int loop;
		private int interval;
		private Connection conn;
		
		public Producer(String servers, int loop, int interval) {
			this.servers = servers;
			this.loop = loop;
			this.interval = interval;			
		}
		
		public int partitionFor(String subject, int numPartitions) {
			try {
				Properties props = new Properties();
				props.setProperty("servers", servers);
				LOG.info("Connecting to : " + servers);
				conn = Connection.connect(props);

				LOG.info("Start publishing " + loop + " updates for " + subject);
				long start = System.currentTimeMillis();
				for(int i = 0; i < loop; i++) {
					Thread.sleep(interval);
					conn.publish(subject, "hello world " + i);
				}
				long end = System.currentTimeMillis();
				LOG.info("Feeding " + Integer.toString(loop) + " updates for subject (" + subject + ") has completed in " + Long.toString(end - start) + "ms");
			}
			catch(Exception e) {
				LOG.error(e.getMessage());
			}
			return 0;
		}
	}
	
	public void startProducers(DataflowPipelineOptions options) throws IOException, InterruptedException {	
		options.setNumWorkers(producers);
		options.setJobName("producers-" + System.currentTimeMillis());
		
		Pipeline p = Pipeline.create(options);
		p
			.apply(Create.of(subjects)).setCoder(StringUtf8Coder.of())
			.apply(Partition.of(subjects.size(), new NatsIOBench.Producer(servers, loop, interval)));
		p.run();		
	}
	
	private static class SimpleFunc extends DoFn<KV<String,String>,String> implements Serializable {
		private static final long serialVersionUID = -1366613943065649148L;

		@Override
		public void processElement(ProcessContext c) throws Exception {
			c.output(c.element().getKey() + "," + c.element().getValue());		
		}
	}
	
	public void startConsumers(DataflowPipelineOptions options) throws IOException, InterruptedException {		
		//options.setStreaming(true);
		options.setNumWorkers(consumers);
		options.setJobName("consumers-" + System.currentTimeMillis());
		
		Properties props = new Properties();
		props.setProperty("servers", servers);
		if (queue != null)
			props.setProperty("queue", queue);
		
		Pipeline p = Pipeline.create(options);
		p
			.apply(NatsIO.Read.withMaxNumRecords(subject, maxRecords, props)) // Batch with max records
			//.apply(NatsIO.Read.withMaxReadtime(subject, maxReadtime, props)) // Batch with max interval
			//.apply(NatsIO.Read.from(subject, props).named("Consumer")) // Streaming
			.apply(ParDo.of(new NatsIOBench.SimpleFunc()));
		p.run();		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		DataflowPipelineOptions consumerOpts = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
		DataflowPipelineOptions producerOpts = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);

		NatsIOBench bench = new NatsIOBench();
		bench.startConsumers(consumerOpts);
		// Making sure consumers to start first and get ready for receiving updates.
		Thread.sleep(30000);
		bench.startProducers(producerOpts);
	}
		
}

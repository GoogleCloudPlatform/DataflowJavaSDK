package com.google.cloud.dataflow.contrib.natsio;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.nats.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.contrib.natsio.example.NatsIOBench;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.BlockingDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

public class NatsIOTest implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(NatsIOBench.class);	
	
	private String servers;
	private String queue;
	
	private String project;
	private String stagingLocation;
	private int maxRecords;
	private int maxReadtime;
	private int loop;
	private int interval;
	private int consumers;
	private int producers;
	private List<String> subjects;
	private String subject;
	
	private Properties props;
	
	public NatsIOTest() {
		servers = System.getProperty("nats.servers");
		queue = System.getProperty("nats.queue");

		project = System.getProperty("project");
		stagingLocation = System.getProperty("stagingLocation");
		
		maxRecords = (System.getProperty("nats.maxRecords") == null ? 30000 : Integer.parseInt(System.getProperty("nats.maxRecords")));
		maxReadtime = (System.getProperty("nats.maxReadtime") == null ? 100 : Integer.parseInt(System.getProperty("nats.maxReadtime")));

		subjects = (System.getProperty("subjects") == null ? Arrays.asList("test") : Arrays.asList(System.getProperty("subjects").split(",")));
		subject =  subjects.get(0);
		
		loop = (System.getProperty("loop") == null ? 30000 : Integer.parseInt(System.getProperty("loop")));
		interval = (System.getProperty("interval") == null ? 0 : Integer.parseInt(System.getProperty("interval")));
		consumers = (System.getProperty("consumers") == null ? 1 :Integer.parseInt(System.getProperty("consumers")));
		producers = (System.getProperty("producers") == null ? 1 : Integer.parseInt(System.getProperty("producers")));
		
		props = new Properties();
		props.setProperty("servers", servers);
		if (queue != null)
			props.setProperty("queue", queue);
	}
	
	
	private BlockingDataflowPipelineOptions buildOptions() {
		BlockingDataflowPipelineOptions options = PipelineOptionsFactory.create().as(BlockingDataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject(project);
		options.setStagingLocation(stagingLocation);
		options.setNumWorkers(1);
		
		return options;
	}
	
	@Test
	public void simplePublish() {
		BlockingDataflowPipelineOptions options = buildOptions();
		options.setJobName("simple-publish-" + System.currentTimeMillis());
		
		Pipeline p = Pipeline.create(options);
		p
			.apply(Create.of(subject)).setCoder(StringUtf8Coder.of())
			.apply(ParDo.of(new DoFn<String, KV<String,String>>() {
				@Override
				public void processElement(DoFn<String, KV<String, String>>.ProcessContext c) throws Exception {
					c.output(KV.of(c.element(), "hello world!!!"));
				}
			}))
			.apply(NatsIO.Write.to(props));
		p.run();
	}
	
	@Test
	public void publishSubscribe() throws InterruptedException {
		// Starting a consumer
		DataflowPipelineOptions consumerOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		consumerOptions.setRunner(DataflowPipelineRunner.class);
		consumerOptions.setProject(project);
		consumerOptions.setStagingLocation(stagingLocation);
		consumerOptions.setNumWorkers(1);
		consumerOptions.setJobName("consumer-" + System.currentTimeMillis());
		
		Pipeline p1 = Pipeline.create(consumerOptions);
		p1
			.apply(NatsIO.Read.withMaxNumRecords(subject, maxRecords, props))
			.apply(ParDo.of(new DoFn<KV<String,String>,String>() {
				@Override
				public void processElement(DoFn<KV<String, String>, String>.ProcessContext c) throws Exception {
					c.output(c.element().getKey() + "," + c.element().getValue());
				}
			}));
		p1.run();

		// Starting a producer
		BlockingDataflowPipelineOptions producerOptions = buildOptions();
		producerOptions.setJobName("producer-" + System.currentTimeMillis());
		
		Pipeline p2 = Pipeline.create(producerOptions);
		p2
			.apply(Create.of(subject)).setCoder(StringUtf8Coder.of())
			.apply(ParDo.of(new DoFn<String, String>() {
				public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
					try {
						Connection conn = Connection.connect(props);
						for(int i = 0; i < loop; i++) {
							conn.publish(subject, Integer.toString(i));
							Thread.sleep(interval);
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}		
					c.output("done");
				}				
			}));
		p2.run();				
	}
}

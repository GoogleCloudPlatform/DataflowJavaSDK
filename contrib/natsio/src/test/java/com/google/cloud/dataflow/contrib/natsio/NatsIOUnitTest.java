package com.google.cloud.dataflow.contrib.natsio;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.*;
import org.nats.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.contrib.natsio.NatsIO;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.BlockingDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;

public class NatsIOUnitTest implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(NatsIOUnitTest.class);	
	
	private String servers;
	private String queue;
	
	private List<String> subjects;
	private String subject;
	
	private Properties props;

	public NatsIOUnitTest() {
		servers = System.getProperty("nats.servers");
		if (servers == null) {
			servers = "nats://127.0.0.1:4222";
		}
		queue = System.getProperty("nats.queue");

		subjects = (System.getProperty("subjects") == null ? Arrays.asList("test") : Arrays.asList(System.getProperty("subjects").split(",")));
		subject =  subjects.get(0);
		
		props = new Properties();
		props.setProperty("servers", servers);
		if (queue != null)
			props.setProperty("queue", queue);
	}
	
	@Test
	public void publish() {
		BlockingDataflowPipelineOptions options = PipelineOptionsFactory.create().as(BlockingDataflowPipelineOptions.class);
		options.setRunner(DirectPipelineRunner.class);
		options.setJobName("publish-" + System.currentTimeMillis());
		
		Pipeline p = DirectPipeline.create(options);
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
	public void subscribe() {
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(DirectPipelineRunner.class);
		options.setJobName("subscribe-" + System.currentTimeMillis());
		
		Thread publisher = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Connection conn = Connection.connect(props);
					// Make sure to wait for a pipeline to start for Read.
					Thread.sleep(3000);
					conn.publish(subject, "hello world!!!");
					conn.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		publisher.start();
		
		Pipeline p = DirectPipeline.create(options);
		p
			.apply(NatsIO.Read.withMaxNumRecords(subject, 1, props));
		p.run();
	}
}

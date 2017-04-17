package com.google.cloud.dataflow.contrib.natsio;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Properties;

import org.junit.*;
import org.junit.rules.ExpectedException;

import com.google.cloud.dataflow.contrib.natsio.NatsIO.Read.NatsSource;
import com.google.cloud.dataflow.contrib.natsio.NatsIO.Write.NatsSink;

public class NatsIOUnitTest implements Serializable {	
	public final ExpectedException thrown = ExpectedException.none();
	
	private static final String DEFAULT_SUBJECT = "test";
	private static final String DEFAULT_SERVER = "nats://127.0.0.1:4222";
	private static final String DEFAULT_QUEUE = "queue1";	
	private Properties props;

	public NatsIOUnitTest() {
		props = new Properties();
		props.setProperty("servers", DEFAULT_SERVER);
		props.setProperty("queue", DEFAULT_QUEUE);
	}
	
	@Test
	public void testBuildSource() {
		NatsSource source = new NatsSource(DEFAULT_SUBJECT, props);
		assertEquals(DEFAULT_SUBJECT, source.getSubject());
		assertEquals(DEFAULT_SERVER, source.getProperties().getProperty("servers"));
		assertEquals(DEFAULT_QUEUE, source.getProperties().getProperty("queue"));
	}
		
	@Test
	public void testSourceNullSubject() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("subject");
		NatsSource source = new NatsSource(null, props);
		source.validate();
	}
	
	@Test
	public void testSourceNullProperties() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("props");
		NatsSource source = new NatsSource(DEFAULT_SUBJECT, null);
		source.validate();
	}

	@Test
	public void testSourceNullServer() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("servers");
		Properties emptyProps = new Properties();
		NatsSource source = new NatsSource(DEFAULT_SUBJECT, emptyProps);
		source.validate();
	}
	
	@Test
	public void testBuildSink() {
		NatsSink sink = new NatsSink(props);
		assertEquals(DEFAULT_SERVER, sink.getProperties().getProperty("servers"));
		assertEquals(DEFAULT_QUEUE, sink.getProperties().getProperty("queue"));		
	}
	
	@Test
	public void testSinkNullProperties() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("props");
		NatsSink sink = new NatsSink(null);
		sink.validate(null);
	}

	@Test
	public void testSinkNullServer() {
		thrown.expect(NullPointerException.class);
		thrown.expectMessage("servers");
		Properties emptyProps = new Properties();
		NatsSink sink = new NatsSink(emptyProps);
		sink.validate(null);
	}
}

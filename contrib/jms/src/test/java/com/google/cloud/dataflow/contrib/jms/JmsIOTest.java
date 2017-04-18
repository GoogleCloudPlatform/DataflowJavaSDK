/*
 * Copyright (C) 2015 The Google Cloud Dataflow Hadoop Library Authors
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


package com.google.cloud.dataflow.contrib.jms;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Test of the JmsIO.
 */
public class JmsIOTest {

    private static final String BROKER_URL = "vm://localhost";

    private BrokerService broker;
    private ConnectionFactory connectionFactory;

    @Before
    public void startBroker() throws Exception {
        System.out.println("Starting ActiveMQ broker on " + BROKER_URL);
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setPersistenceAdapter(new MemoryPersistenceAdapter());
        broker.addConnector(BROKER_URL);
        broker.setBrokerName("localhost");
        broker.start();

        // create JMS connection factory
        System.out.println("Create JMS connection factory on " + BROKER_URL);
        connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
    }

    @Test
    public void testReadSingleMessage() throws Exception {

        // produce message
        System.out.println("Producing a test message on the broker ...");
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(session.createQueue("test"));
        TextMessage message = session.createTextMessage("This Is A Test");
        producer.send(message);
        producer.send(message);
        producer.send(message);
        producer.send(message);
        producer.send(message);
        producer.send(message);
        producer.close();
        session.close();
        connection.close();

        System.out.println("Creating test pipeline");
        Pipeline pipeline = TestPipeline.create();

        // read from the queue
        System.out.println("Reading from the JMS queue and create PCollection");
        PCollection<String> output =  pipeline.apply(
                JmsIO.Read
                        .connectionFactory(connectionFactory)
                        .queue("test")
                        .maxNumMessages(5));

        System.out.println("Starting the pipeline");
        pipeline.run();

        System.out.println("Check assertion");
        DataflowAssert.that(output).containsInAnyOrder(new String[]{ "test "});

        System.out.println(output);
    }

    @Test
    public void testWriteMessage() throws Exception {
        System.out.println("Create test pipeline");
        Pipeline pipeline = TestPipeline.create();

        System.out.println("Create PCollection");
        ArrayList<String> data = new ArrayList<>();
        data.add("Test");
        PCollection<String> input = pipeline.apply(Create.of(data).withCoder(StringUtf8Coder.of()));

        System.out.println("Write PCollection to the JMS queue");
        JmsIO.Write.Bound<String> write = JmsIO.Write
                .connectionFactory(connectionFactory)
                .queue("test");

        input.apply(write);

        pipeline.run();

        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer messageConsumer = session.createConsumer(session.createQueue("test"));
        TextMessage message = (TextMessage) messageConsumer.receive(10000);
        Assert.assertEquals(message.getText(), "Test");
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
    }

}

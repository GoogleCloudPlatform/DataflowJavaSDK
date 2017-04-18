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

package com.google.cloud.dataflow.contrib.jms;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Read and Write {@link PTransform}s for JMS broker. These transforms create and consume
 * unbounded {@link PCollection PCollections}.
 */
public class JmsIO {

    private static final Logger LOG = LoggerFactory.getLogger(JmsIO.class);

    /** The default {@link Coder} used to translate to/from JMS messages. */
    public static final Coder<String> DEFAULT_JMS_CODER = StringUtf8Coder.of();

    /**
     * A {@link PTransform} that continuously reads from a JMS broker and
     * return a {@link PCollection} of {@link String Strings} containing the items.
     */
    public static class Read {

        /**
         * Creates and returns a transform for reading from the JMS broker with the specified
         * transform name.
         *
         * @param name the step name
         * @return the Bound
         */
        public static Bound<String> named(String name) {
            return new Bound<>(DEFAULT_JMS_CODER).named(name);
        }

        /**
         * Creates and returns a transform for reading from a JMS broker with the provided
         * JMS connection factory.
         *
         * @param connectionFactory the JMS connection factory to use to connect to the broker
         * @return the Bound
         */
        public static Bound<String> connectionFactory(ConnectionFactory connectionFactory) {
            return new Bound<>(DEFAULT_JMS_CODER).connectionFactory(connectionFactory);
        }

        /**
         * Creates and returns a transform for reading from a JMS broker with the provided
         * JMS queue.
         *
         * @param name the JMS queue name
         * @return the Bound
         */
        public static Bound<String> queue(String name) {
            return new Bound<>(DEFAULT_JMS_CODER).queue(name);
        }

        /**
         * Creates and returns a transform for reading from a JMS broker with the provided
         * JMS topic.
         *
         * @param name the JMS topic name.
         * @return the Bound
         */
        public static Bound<String> topic(String name) {
            return new Bound<>(DEFAULT_JMS_CODER).topic(name);
        }

        /**
         * Creates and returns a transform for reading from JMS broker that uses the given
         * {@link Coder} to decode JMS messages into a value of type {@code T}.
         *
         * @param coder the coder to use
         * @return the Bound
         */
        public static <T> Bound<T> withCoder(Coder<T> coder) {
            return new Bound<>(coder);
        }

        /**
         * Creates and returns a tranform for reading from JMS broker that limit the
         * number of messages consumed.
         *
         * @param maxNumMessages the max number of messages consumed
         * @return the Bound
         */
        public static Bound<String> maxNumMessages(int maxNumMessages) {
            return new Bound<>(DEFAULT_JMS_CODER).maxNumMessages(maxNumMessages);
        }

        /**
         * A {@link PTransform} that reads from a JMS broker and returns
         * a unbounded {@link PCollection} containing the items from the stream.
         */
        public static class Bound<T> extends PTransform<PInput, PCollection<T>> {

            /** The JMS connection factory. */
            private final ConnectionFactory connectionFactory;

            /** The JMS queue to read from. */
            private final String queue;

            /** The JMS topic to read from. */
            private final String topic;

            /** The coder used to decode each message. */
            @Nullable
            private final Coder<T> coder;

            /** Stop after reading this many messages. */
            private final int maxNumMessages;

            private Bound(Coder<T> coder) {
                this(null, null, null, null, coder, 0);
            }

            private Bound(String name, ConnectionFactory connectionFactory, String queue,
                          String topic, Coder<T> coder, int maxNumMessages) {
                super(name);
                this.connectionFactory = connectionFactory;
                this.queue = queue;
                this.topic = topic;
                this.coder = coder;
                this.maxNumMessages = maxNumMessages;
            }

            /**
             * Returns a transform that's like this one but with the given step name.
             *
             * @param name the step name
             * @return the Bound
             */
            public Bound<T> named(String name) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder, maxNumMessages);
            }

            /**
             * Returns a transform that's like this one but using the JMS connection factory.
             *
             * @param connectionFactory the JMS connection factory to use
             * @return the Bound
             */
            public Bound<T> connectionFactory(ConnectionFactory connectionFactory) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder, maxNumMessages);
            }

            /**
             * Returns a transform that's like this one but that reads
             * from the specified JMS queue.
             *
             * @param queue the JMS queue
             * @return the Bound
             */
            public Bound<T> queue(String queue) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder, maxNumMessages);
            }

            /**
             * Returns a transform that's like this one but that reads
             * from the specified JMS topic.
             *
             * @param topic the JMQ topic
             * @return the Bound
             */
            public Bound<T> topic(String topic) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder, maxNumMessages);
            }

            /**
             * Returns a transform that's like this one but that uses the
             * given {@link Coder} to decode each record into a value of
             * type {@code X}.
             *
             * @param coder the coder to use
             * @return the Bound
             */
            public <X> Bound<X> withCoder(Coder<X> coder) {
                return new Bound<>(name, connectionFactory, queue, topic, coder, maxNumMessages);
            }

            /**
             * Return a transform that's like this one but will only read up
             * to the specified maximum number of messages from the JMS broker.
             * The transform produces a <i>bounded</i> {@link PCollection}.
             *
             * @param maxNumMessages the max number messages limit to consume
             * @return the Bound
             */
            public Bound<T> maxNumMessages(int maxNumMessages) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder, maxNumMessages);
            }

            @Override
            public PCollection<T> apply(PInput input) {
                if (connectionFactory == null) {
                    throw new IllegalArgumentException("need to set connection factory " +
                            "for a JmsIO.Read transform");
                }
                if (queue == null && topic == null) {
                    throw new java.lang.IllegalStateException("need to set destination " +
                            "(queue or topic) for a JmsIO.Read transform");
                }

                boolean boundedOutput = getMaxNumMessages() > 0;

                if (boundedOutput) {
                    return input.getPipeline().begin()
                            .apply(Create.of((Void) null)).setCoder(VoidCoder.of())
                            .apply(ParDo.of(new JmsReader())).setCoder(coder);
                } else {
                    return PCollection.<T>createPrimitiveOutputInternal(
                            input.getPipeline(), WindowingStrategy.globalDefault(),
                            PCollection.IsBounded.UNBOUNDED).setCoder(coder);
                }
            }

            @Override
            protected Coder<T> getDefaultOutputCoder() {
                return coder;
            }

            public ConnectionFactory getConnectionFactory() {
                return connectionFactory;
            }

            public String getQueue() {
                return queue;
            }

            public String getTopic() {
                return topic;
            }

            public Coder<T> getCoder() {
                return coder;
            }

            public int getMaxNumMessages() {
                return maxNumMessages;
            }

            private class JmsReader extends DoFn<Void, T> {

                @Override
                public void processElement(ProcessContext c) throws Exception {
                    LOG.debug("Connecting to the JMS broker");
                    Connection connection = connectionFactory.createConnection();
                    connection.start();
                    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    MessageConsumer consumer;
                    if (queue != null) {
                        consumer = session.createConsumer(session.createQueue(queue));
                    } else {
                        consumer = session.createConsumer(session.createTopic(topic));
                    }

                    List<TextMessage> messages = new ArrayList<>();

                    while ((getMaxNumMessages() == 0 ||
                            messages.size() <= getMaxNumMessages())) {
                        LOG.debug("Consuming JMS message");
                        TextMessage message = (TextMessage) consumer.receive();
                        LOG.debug("Adding JMS message to the list ({})", messages.size());
                        messages.add(message);
                    }

                    consumer.close();
                    session.close();
                    connection.stop();
                    connection.close();

                    for (TextMessage message : messages) {
                        c.output(CoderUtils.decodeFromByteArray(coder,
                                message.getText().getBytes()));
                    }
                }

            }


        }

        /** Disallow construction of utility class. */
        private Read() {}

    }

    /** Disallow construction of utility class. */
    private JmsIO() {}

    /**
     * A {@link PTransform} that continuously writes a {@link PCollection} of {@link String Strings}
     * to a JMS broker on the specified destination.
     */
    public static class Write {

        /**
         * Creates a transform that produces to a JMS broker with the given step name.
         *
         * @param name the step name
         * @return the Bound
         */
        public static Bound<String> named(String name) {
            return new Bound<>(DEFAULT_JMS_CODER).named(name);
        }

        /**
         * Creates a transform that produces to a JMS broker with the given JMS connection factory.
         *
         * @param connectionFactory the JMS connection factory to use to connect to the JMS broker
         * @return the Bound
         */
        public static Bound<String> connectionFactory(ConnectionFactory connectionFactory) {
            return new Bound<>(DEFAULT_JMS_CODER).connectionFactory(connectionFactory);
        }

        /**
         * Creates a transform that produces to a JMS broker
         * on the given JMS queue.
         *
         * @param queue the JMS queue where to produce messages
         * @return the Bound
         */
        public static Bound<String> queue(String queue) {
            return new Bound<>(DEFAULT_JMS_CODER).queue(queue);
        }

        /**
         * Creates a trasnform that produces to a JMS broker
         * on the given JMS topic.
         *
         * @param topic the JMS topic where to produce messages
         * @return the Bound
         */
        public static Bound<String> topic(String topic) {
            return new Bound<>(DEFAULT_JMS_CODER).topic(topic);
        }

        /**
         * Creates a transform that uses the given {@link Coder}
         * to encode each of the elements of the input collection
         * into an output message.
         *
         * @param coder the coder to use
         * @return the Bound
         */
        public static <T> Bound<T> withCoder(Coder<T> coder) {
            return new Bound<>(coder);
        }

        /**
         * A {@link PTransform} that reads from a JMS broker and returns
         * a unbounded {@link PCollection} containing the items from the stream.
         */
        public static class Bound<T> extends PTransform<PCollection<T>, PDone> {

            /**
             * The JMS connection factory.
             */
            private final ConnectionFactory connectionFactory;

            /**
             * The JMS queue where to produce messages.
             */
            private final String queue;

            /**
             * The JMS topic where to produce messages.
             */
            private final String topic;

            /**
             * The coder used to decode each message.
             */
            @Nullable
            private final Coder<T> coder;

            private Bound(Coder<T> coder) {
                this(null, null, null, null, coder);
            }

            private Bound(String name, ConnectionFactory connectionFactory,
                          String queue, String topic, Coder<T> coder) {
                super(name);
                this.connectionFactory = connectionFactory;
                this.queue = queue;
                this.topic = topic;
                this.coder = coder;
            }

            public Bound<T> named(String name) {
                return new Bound<T>(name,
                        connectionFactory, queue, topic, coder);
            }

            public Bound<T> connectionFactory(ConnectionFactory connectionFactory) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder);
            }

            public Bound<T> queue(String queue) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder);
            }

            public Bound<T> topic(String topic) {
                return new Bound<T>(name, connectionFactory, queue, topic, coder);
            }

            public <X> Bound<X> withCoder(Coder<X> coder) {
                return new Bound<>(name, connectionFactory, queue, topic, coder);
            }

            @Override
            public PDone apply(PCollection<T> input) {
                if (connectionFactory == null) {
                    throw new java.lang.IllegalStateException("need to set the JMS " +
                            "connection factory of a " +
                            "JmsIO.Write transform");
                }
                if (queue == null && topic == null) {
                    throw new java.lang.IllegalStateException("need to set the JMS " +
                            "destination (queue or topic) of " +
                            "a JmsIO.Write transform");
                }
                input.apply(ParDo.of(new JmsWriter()));
                return PDone.in(input.getPipeline());
            }

            @Override
            protected Coder<T> getDefaultOutputCoder() {
                return coder;
            }

            public ConnectionFactory getConnectionFactory() {
                return connectionFactory;
            }

            public String getQueue() {
                return queue;
            }

            public String getTopic() {
                return topic;
            }

            public Coder<T> getCoder() {
                return coder;
            }

            private class JmsWriter extends DoFn<T, Void> {
                private transient Connection connection;
                private transient Session session;

                @Override
                public void startBundle(Context c) throws Exception {
                    connection = connectionFactory.createConnection();
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                }

                @Override
                public void processElement(ProcessContext c) throws Exception {
                    TextMessage message = session.createTextMessage(
                            (String) CoderUtils.decodeFromByteArray(
                                    getCoder(),
                                    ((String) c.element()).getBytes()));
                    MessageProducer producer;
                    if (queue != null) {
                        producer = session.createProducer(session.createQueue(queue));
                    } else {
                        producer = session.createProducer(session.createTopic(topic));
                    }
                    producer.send(message);
                    producer.close();
                }

                @Override
                public void finishBundle(Context c) throws Exception {
                    if (session != null) {
                        session.close();
                    }
                    if (connection != null) {
                        connection.close();
                    }
                }

            }

        }

        /** Disallow construction of utility class. */
        private Write() {}

    }

}

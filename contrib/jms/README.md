# JMS module

This library provides Dataflow sources and sinkgs to make it possible to read
and write on JMS brokers from Dataflow pipelines.

It supports both JMS queues and topics, with unbounded or bounded `PCollections`.

To use JmsIO, you have to:

1. Create a JMS `ConnectionFactory` specific to the JMS broker you want to use (for instance, ActiveMQ, IBM MQ, ...)
2. Specify the JMS destination (queue or topic) you want to use

## Reading (consuming messages) with JmsIO

The `JmsIO.Read` transform continuously consumes from the JMS broker and returns an unbounded `PCollection` of `Strings` that
represent the messages. By default, each element in the resulting `PCollection` is encoded as a UTF-8 string.
You can override the default encoding by using `withCoder` when you call JmsIO.Read.

----
PipelineOptions options = PipelineOptionsFactory.create();
Pipeline p = Pipeline.create(options);

// create JMS connection factory, for instance, with ActiveMQ
javax.jms.ConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");

PCollection<String> data = p.apply(JmsIO.Read.named("ConsumeFromJMS").connectionFactory(connectionFactory).queue("my-queue"));
----

### Reading a bounded set of messages from JMS

The `DirectPipelineRunner` and the batch mode of the Dataflow service do not support unbounded `PCollections`.
To use JmsIO as source in these contexts, you need to supply a bound on the amount of messages to consume.

You can specify the `.maxNumMessages` option to read a fixed maximum number of messages.

## Writing (producing messages) with JmsIO

The JmsIO.Write transform continuously writes an unbounded `PCollection` of `String` objects, produced to a
JMS broker. By default, the input `PCollection` to `JmsIO.Write` must contain strings encoded in UTF-8.
You can change the expected input type and encoding by using `withCoder`.

----
PCollection<String> data = ...;
data.apply(JmsIO.Write.named("ProduceToJMS").connectionFactory(connectionFactory).queue("my-queue"));
----
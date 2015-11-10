# natsio

Google Cloud Dataflow Java Connector for [NATS messaging system](http://nats.io).

## Getting Started

Clone Dataflow SDK with natsio Connector and built them with Maven.

Install and launch [NATS server](http://nats.io/documentation/server/gnatsd-intro/), typically on Google Compute Engine.
```bash
% gnatsd
```

Run tests with Maven. The following command will launch a producer and consumer job respectively.<BR>
The producer publishes messages and the consumer will receive them via NATS server.<BR>

To build (set "-DskipTests=false" for unit testing)
```bash
% mvn install
```

To run as a stand-alone application with Dataflow service<BR>
"stagingLocation", "project" and "nats.servers" are mandatory properties to run a test.
```bash
% mvn exec:java -Dexec.mainClass="com.google.cloud.dataflow.contrib.natsio.example.NatsIOTest" ¥
-DstagingLocation=gs://<bucket> -Dproject=<project id> -Dnats.servers=nats://<server>:4222 -Dnats.queue=queue1 ¥
-Dloop=30000 -Dinterval=0 -Dsubjects=test1 -Dconsumers=1 -Dproducers=1 -Dnats.maxRecords=20000 ¥
-Dnats.maxReadtime=30
```

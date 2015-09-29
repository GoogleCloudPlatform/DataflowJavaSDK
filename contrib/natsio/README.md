# natsio

Google Cloud Dataflow Java Connector for [NATS messaging system](http://nats.io).

## Getting Started

Clone Dataflow SDK with natsio Connector and built them with Maven.

Install and launch [NATS server](http://nats.io/docs/#gnatsd), typically on Google Compute Engine.
```bash
% gnatsd
```

Run tests with Maven. The following command will launch a producer and consumer job respectively.<BR>
The producer publishes messages and the consumer will receive them via NATS server.
```bash
<<<<<<< HEAD
% mvn test -DstagingLocation=gs://<bucket> -Dproject=<project id> -Dnats.servers=nats://<server>:4222 -Dnats.queue=queue1 -Dloop=30000 -Dinterval=0 -Dsubjects=test1 -Dconsumers=1 -Dproducers=1 -Dnats.maxRecords=20000
=======
% mvn test -Dtest=NatsIOTest#publishSubscribe -DstagingLocation=gs://<bucket> ¥
-Dproject=<project id> -Dnats.servers=nats://<server>:4222 -Dnats.queue=queue1 ¥
-Dloop=30000 -Dinterval=0 -Dsubjects=test1 -Dconsumers=1 -Dproducers=1 -Dnats.maxRecords=20000 ¥
-Dnats.maxReadtime=30
>>>>>>> 56ea0c2b2f1de2aceb38ebbe020a3aa6941a6555
```

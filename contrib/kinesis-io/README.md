# User guide to Kinesis source for DataFlow

Library, which allows to use [Kinesis](https://aws.amazon.com/kinesis/) as a source for Google DataFlow pipeline.

## Usage

Main class you're going to operate is called KinesisIO. It follows the usage conventions laid out by other *IO classes like BigQueryIO or PubsubIO

Let's see how you can set up a simple Pipeline, which reads from Kinesis:


    p.apply(KinesisIO.Read.
            from("streamName", InitialPositionInStream.LATEST).
            using("AWS_KEY", _"AWS_SECRET", STREAM_REGION).
    apply( ... ) // other transformations

As you can see you need to provide 3 things:

* name of the stream you're going to read
* position in the stream where reading should start. There are two options:
    * LATEST - reading will begin from end of the stream
    * TRIM_HORIZON - reading will begin at the very beginning of the stream
* data used to initialize Kinesis client
    * credentials (aws key, aws secret)
    * region where the stream is located

In case when you want to set up Kinesis client by your own (for example if you're using
more sophisticated authorization methods like Amazon STS, etc.) you can do it by
implementing KinesisClientProvider class:

    public class MyCustomKinesisClientProvider implements KinesisClientProvider {
        @Override
        public AmazonKinesis get() {
            // set up your client here
        }
    }

Usage is pretty straightforward:

    p.apply(KinesisIO.Read.
            from("streamName", InitialPositionInStream.LATEST).
            using(MyCustomKinesisClientProvider()).
    apply( ... ) // other transformations
    
## Build

Just invoke in the main directory:

    ./gradlew jar
    
Library jar should be created in `build/libs` directory.

## Future work

* add Kinesis sink, i.e. KinesisIO.Writer
* handle shard splitting / merging events

# Developer guide

## Why are we not using [Kinesis Client Library](https://github.com/awslabs/amazon-kinesis-client) under the hood?
There are few reasons for this:

* KCL operates in "push" manner, while Dataflow Reader is "pull"-based. It'd be hard to combine those two approaches.
* KCL does checkpointing in Amazon DynamoDB. Completely unnecessary given the internal checkpointing mechanism of Dataflow.
* KCL spawns many threads under-the-hood to do the job. I'm not sure if this exactly the best idea to do inside Kinesis Dataflow Reader
    

## Running integration / E2E tests

Both integration and E2E tests are connecting to the real Kinesis service, so in order to run them you need
to provide Amazon credentials and test stream details in `src/test/resources/testconfig.json` file:

    {
      "AWS_SECRET_KEY": "******",
      "AWS_ACCESS_KEY": "******",
      "TEST_KINESIS_STREAM": "******",
      "TEST_KINESIS_STREAM_REGION": "eu-west-1"
      // other details
    }

To run them just do:

    ./gradlew clean integration
    
E2E tests (in contrary to integration tests) also run real dataflow job remotely. This means you need a Google project with Compute Engine, Dataflow, Google Cloud Storage and BigQuery services configured and also provide additional details in `src/test/resources/testconfig.json` file:

    {
      // amazon properties here
      "DATAFLOW_TEST_BUCKET": "test_bucket",
      "DATAFLOW_TEST_PROJECT": "project",
      "DATAFLOW_TEST_DATASET": "test_dataflow_bq_dataset"
    }
    
To run them just do:

    ./gradlew clean e2e
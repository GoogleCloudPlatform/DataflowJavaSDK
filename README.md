<!--
  Copyright (C) 2017 Google Inc.

  Licensed under the Apache License, Version 2.0 (the "License"); you may not
  use this file except in compliance with the License. You may obtain a copy of
  the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations under
  the License.
-->

# Google Cloud Dataflow SDK for Java

[Google Cloud Dataflow](https://cloud.google.com/dataflow/) provides a simple,
powerful programming model for building both batch and streaming parallel data
processing pipelines.

Dataflow SDK for Java is a distribution of a portion of the
[Apache Beam](https://beam.apache.org) project. This repository hosts the
code to build this distribution and any Dataflow-specific code/modules. The
underlying source code is hosted in the
[Apache Beam repository](https://github.com/apache/beam).

[General usage](https://cloud.google.com/dataflow/getting-started) of Google
Cloud Dataflow does **not** require use of this repository. Instead, you can do
any one of the following:

1. Depend directly on a specific
[version](https://cloud.google.com/dataflow/downloads) of the SDK in
the [Maven Central Repository](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.google.cloud.dataflow%22)
by adding the following dependency to development
environments like Eclipse or Apache Maven:

        <dependency>
          <groupId>com.google.cloud.dataflow</groupId>
          <artifactId>google-cloud-dataflow-java-sdk-all</artifactId>
          <version>version_number</version>
        </dependency>

1. Download the example pipelines from the separate
[DataflowJavaSDK-examples](https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples)
repository.

1. If you are using [Eclipse](https://eclipse.org/) integrated development
environment (IDE), the
[Cloud Dataflow Plugin for Eclipse](https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-eclipse)
provides tools to create and execute Dataflow pipelines inside Eclipse.

## Status [![Build Status](https://api.travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK)

Both the SDK and the Dataflow Service are generally available and considered
stable and fully qualified for production use.

This [`master`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/) branch
contains code to build Dataflow SDK 2.0.0 and newer, as a distribution of Apache
Beam. Pre-Beam SDKs, versions 1.x, are maintained in the
[`master-1.x`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/tree/master-1.x)
branch.

## Overview

The key concepts in this programming model are:

* `PCollection`: represents a collection of data, which could be bounded or
unbounded in size.
* `PTransform`: represents a computation that transforms input PCollections
into output PCollections.
* `Pipeline`: manages a directed acyclic graph of PTransforms and PCollections
that is ready for execution.
* `PipelineRunner`: specifies where and how the pipeline should execute.

We provide two runners:

  1. The `DirectRunner` runs the pipeline on your local machine.
  1. The `DataflowRunner` submits the pipeline to the Cloud Dataflow Service,
where it runs using managed resources in the
[Google Cloud Platform](https://cloud.google.com).

The SDK is built to be extensible and support additional execution environments
beyond local execution and the Google Cloud Dataflow Service. Apache Beam
contains additional SDKs, runners, and IO connectors.

## Getting Started

Please try our [Quickstarts](https://cloud.google.com/dataflow/docs/quickstarts).

## Contact Us

We welcome all usage-related questions on [Stack Overflow](http://stackoverflow.com/questions/tagged/google-cloud-dataflow)
tagged with `google-cloud-dataflow`.

Please use [issue tracker](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues)
on GitHub to report any bugs, comments or questions regarding SDK development.

## More Information

* [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
* [Apache Beam](https://beam.apache.org/)
* [Dataflow Concepts and Programming Model](https://beam.apache.org/documentation/programming-guide/)
* [Java API Reference](https://beam.apache.org/documentation/sdks/javadoc/)

_Apache, Apache Beam and the orange letter B logo are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries._

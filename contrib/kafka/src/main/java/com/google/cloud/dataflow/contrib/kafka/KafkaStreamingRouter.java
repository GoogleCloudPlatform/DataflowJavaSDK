/**
 * Sample job that reads from a Kafka topic and submits to BigQuery.
 * This job will form the basis of a streaming router that pulls messages
 * from Kafka and writes them to downstream systems.
 */

package com.google.cloud.dataflow.contrib.kafka;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.workiva.cloud.dataflow.coders.ConsumerRecordCoder;
import com.workiva.cloud.dataflow.io.KafkaSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Echo messages from Kafka to BigQuery.
 */
public class KafkaStreamingRouter {

  static final int WINDOW_SIZE = 1;  // Default window duration in minutes

  /**
   * Prepares the data for writing to BigQuery by building a TableRow object.
   */
  static class FormatMessageFn extends DoFn<ConsumerRecord<String, String>, TableRow> {
  private static final long serialVersionUID = 1L;

  @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set("topic", c.element().topic())
          .set("offset", c.element().offset())
          .set("partition", c.element().partition())
          .set("value", c.element().value());

      c.output(row);
    }
  }

  /** A PTransform DoFn that combines keys and values to one string. */
  static class ConsumerRecordToRowConverter
      extends PTransform<PCollection<ConsumerRecord<String, String>>, PCollection<TableRow>> {
    private static final long serialVersionUID = 1L;

    @Override
    public PCollection<TableRow> apply(PCollection<ConsumerRecord<String, String>> messages) {
      PCollection<TableRow> results = messages.apply(
          ParDo.of(new FormatMessageFn()));

      return results;
    }

  }

   /**
   * Options supported by {@link KafkaStreamingRouterOptions}.
   *
   * <p> Inherits standard configuration options.
   */
  public static interface KafkaStreamingRouterOptions extends DataflowPipelineOptions {
    @Description("Kafka servers in the cluster.")
    @Default.String("127.0.0.0:9092")
    String getBootstrapServers();
    void setBootstrapServers(String value);

    @Description("The group id of these consumers.")
    @Default.String("test")
    String getGroupId();
    void setGroupId(String value);

    @Description("The topic to read.")
    @Default.String("test")
    String getTopic();
    void setTopic(String value);
  }

  public static void main(String[] args) throws IOException {
    KafkaStreamingRouterOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(KafkaStreamingRouterOptions.class);
    options.setProject("cloud-project-namew");
    options.setStreaming(true);
    options.setNumWorkers(3);
    options.setMaxNumWorkers(3);

    String tableSpec = new StringBuilder()
        .append(options.getProject()).append(":")
        .append("kafkastreamingrouter").append(".")
        .append("output")
        .toString();

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("topic").setType("STRING"));
    fields.add(new TableFieldSchema().setName("offset").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("partition").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("key").setType("STRING"));
    fields.add(new TableFieldSchema().setName("value").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    Pipeline pipeline = Pipeline.create(options);

    CoderRegistry cr = pipeline.getCoderRegistry();
    cr.registerCoder(ConsumerRecord.class, ConsumerRecordCoder.class);

    pipeline
      .apply(KafkaSource.readFrom(options.getTopic(), options.getBootstrapServers(), options.getGroupId()))
      .apply(Window.<ConsumerRecord<String,String>>into(FixedWindows.of(
              Duration.standardMinutes(WINDOW_SIZE))))
      .apply(new ConsumerRecordToRowConverter())
      .apply(BigQueryIO.Write.to(tableSpec)
            .withSchema(schema)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    pipeline.run();
  }
}

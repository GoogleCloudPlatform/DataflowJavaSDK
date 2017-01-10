package com.google.cloud.dataflow.contrib.kafka.coders;

import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.codehaus.jackson.annotate.JsonCreator;

/**
 * A {@code ConsumerRecordCoder} encodes Kafka ConsumerRecord objects.
 */
public class ConsumerRecordCoder extends CustomCoder<ConsumerRecord<String, String>> {
  private static final long serialVersionUID = 1L;


  @JsonCreator
  public static ConsumerRecordCoder of() {
    return INSTANCE;
  }

  private static final ConsumerRecordCoder INSTANCE = new ConsumerRecordCoder();

  private ConsumerRecordCoder() {}

  @Override
  public void encode(ConsumerRecord<String, String> value, OutputStream outStream, Context context)
      throws IOException {
    Gson gson = new Gson();
    String json = gson.toJson(value);
    outStream.write(json.getBytes(Charsets.UTF_8));
  }

  @Override
  public ConsumerRecord<String, String> decode(InputStream inStream, Context context)
      throws IOException {
    String json;
    try (InputStreamReader reader = new InputStreamReader(inStream, Charsets.UTF_8)){
      json = CharStreams.toString(reader);
    }
    Gson gson = new Gson();
    @SuppressWarnings("unchecked")
    ConsumerRecord<String, String> record = gson.fromJson(json, ConsumerRecord.class);
    return record;
  }
}

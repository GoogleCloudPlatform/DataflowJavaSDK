package com.workiva.cloud.dataflow.io;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.workiva.cloud.dataflow.coders.ConsumerRecordCoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test case for {@link KafkaCheckpoint}.
 */
@RunWith(JUnit4.class)
public class KafkaCheckpointTest {

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    ConsumerRecordCoder coder = ConsumerRecordCoder.of();

    ConsumerRecord<String, String> record =
        new ConsumerRecord<String, String>("test", 10, 100, "key", "value");

    for (Coder.Context context : Arrays.asList(Coder.Context.OUTER, Coder.Context.NESTED)) {
      ConsumerRecord<String, String> value = decodeEncode(coder, context, record);
      assertEquals(value.topic(), record.topic());
      assertEquals(value.partition(), record.partition());
      assertEquals(value.offset(), record.offset());
      assertEquals(value.key(), record.key());
      assertEquals(value.value(), record.value());
    }
  }

  private static <T> byte[] encode(
      Coder<T> coder, Coder.Context context, T value) throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    Coder<T> deserializedCoder = Serializer.deserialize(coder.asCloudObject(), Coder.class);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    deserializedCoder.encode(value, os, context);
    return os.toByteArray();
  }

  private static <T> T decode(
      Coder<T> coder, Coder.Context context, byte[] bytes) throws CoderException, IOException {
    @SuppressWarnings("unchecked")
    Coder<T> deserializedCoder = Serializer.deserialize(coder.asCloudObject(), Coder.class);

    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    return deserializedCoder.decode(is, context);
  }

  private static <T> T decodeEncode(Coder<T> coder, Coder.Context context, T value)
      throws CoderException, IOException {
    return decode(coder, context, encode(coder, context, value));
  }
}

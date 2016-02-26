package com.google.cloud.dataflow.contrib.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.common.collect.ImmutableList;

public class KafkaRecordCoder<K, V> extends StandardCoder<KafkaRecord<K, V>> {

  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final VarLongCoder longCoder = VarLongCoder.of();
  private static final VarIntCoder intCoder = VarIntCoder.of();

  private final Coder<K> keyCoder;
  private final Coder<V> valueCoder;

  @JsonCreator
  public static KafkaRecordCoder<?, ?> of(@JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
                                          List<Coder<?>> components) {
    KvCoder<?, ?> kvCoder = KvCoder.of(components);
    return of(kvCoder.getKeyCoder(), kvCoder.getValueCoder());
  }

  public static <K, V> KafkaRecordCoder<K, V> of(Coder<K> keyCoder, Coder<V> valueCoder) {
    return new KafkaRecordCoder<K, V>(keyCoder, valueCoder);
  }

  public KafkaRecordCoder(Coder<K> keyCoder, Coder<V> valueCoder) {
    this.keyCoder = keyCoder;
    this.valueCoder = valueCoder;
  }

  @Override
  public void encode(KafkaRecord<K, V> value, OutputStream outStream, Context context)
                         throws CoderException, IOException {
    Context nested = context.nested();
    stringCoder.encode(value.getTopic(),      outStream, nested);
    intCoder   .encode(value.getPartition(),  outStream, nested);
    longCoder  .encode(value.getOffset(),     outStream, nested);
    keyCoder   .encode(value.getKey(),        outStream, nested);
    valueCoder .encode(value.getValue(),      outStream, nested);
  }

  @Override
  public KafkaRecord<K, V> decode(InputStream inStream, Context context)
                                      throws CoderException, IOException {
    Context nested = context.nested();
    return new KafkaRecord<K, V>(
        stringCoder .decode(inStream, nested),
        intCoder    .decode(inStream, nested),
        longCoder   .decode(inStream, nested),
        keyCoder    .decode(inStream, nested),
        valueCoder  .decode(inStream, nested));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(keyCoder, valueCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    verifyDeterministic("Key and Value coder should be deterministic", keyCoder, valueCoder);
  }

  @Override
  public boolean isRegisterByteSizeObserverCheap(KafkaRecord<K, V> value, Context context) {
    return keyCoder.isRegisterByteSizeObserverCheap(value.getKey(), context.nested())
        && valueCoder.isRegisterByteSizeObserverCheap(value.getValue(), context.nested());
    //XXX don't we have to implement getEncodedSize()?
  }

  @Override
  public Object structuralValue(KafkaRecord<K, V> value) throws Exception{
    if (consistentWithEquals())
      return value;
    else
      return new KafkaRecord<Object, Object>(
          value.getTopic(),
          value.getPartition(),
          value.getOffset(),
          keyCoder.structuralValue(value.getKey()),
          valueCoder.structuralValue(value.getValue()));
  }

  @Override
  public boolean consistentWithEquals() {
    return keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals();
  }
}

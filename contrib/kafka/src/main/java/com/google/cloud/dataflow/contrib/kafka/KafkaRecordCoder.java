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

package com.google.cloud.dataflow.contrib.kafka;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * {@link Coder} for {@link KafkaRecord}.
 */
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
    stringCoder.encode(value.getTopic(), outStream, nested);
    intCoder.encode(value.getPartition(), outStream, nested);
    longCoder.encode(value.getOffset(), outStream, nested);
    keyCoder.encode(value.getKey(), outStream, nested);
    valueCoder.encode(value.getValue(), outStream, nested);
  }

  @Override
  public KafkaRecord<K, V> decode(InputStream inStream, Context context)
                                      throws CoderException, IOException {
    Context nested = context.nested();
    return new KafkaRecord<K, V>(
        stringCoder.decode(inStream, nested),
        intCoder.decode(inStream, nested),
        longCoder.decode(inStream, nested),
        keyCoder.decode(inStream, nested),
        valueCoder.decode(inStream, nested));
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
    //TODO : do we have to implement getEncodedSize()?
  }

  @Override
  public Object structuralValue(KafkaRecord<K, V> value) throws Exception {
    if (consistentWithEquals()) {
      return value;
    } else {
      return new KafkaRecord<Object, Object>(
          value.getTopic(),
          value.getPartition(),
          value.getOffset(),
          keyCoder.structuralValue(value.getKey()),
          valueCoder.structuralValue(value.getValue()));
    }
  }

  @Override
  public boolean consistentWithEquals() {
    return keyCoder.consistentWithEquals() && valueCoder.consistentWithEquals();
  }
}

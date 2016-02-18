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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.AbstractBoundedReaderIterator;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.annotation.Nullable;

/**
 * A source that reads Avro files. Records are read from the Avro file as a
 * series of byte arrays. The coder provided is used to deserialize each record
 * from a byte array.
 *
 * @param <T> the type of the elements read from the source
 */
public class AvroByteReader<T> extends Reader<T> {
  final AvroReader<ByteBuffer> avroReader;
  final Coder<T> coder;
  private final Schema schema = Schema.create(Schema.Type.BYTES);

  public AvroByteReader(String filename, @Nullable Long startPosition, @Nullable Long endPosition,
      Coder<T> coder, @Nullable PipelineOptions options) {
    this.coder = coder;
    avroReader = new AvroReader<>(
        filename, startPosition, endPosition, AvroCoder.of(ByteBuffer.class, schema), options);
  }

  @Override
  public ReaderIterator<T> iterator() throws IOException {
    return new AvroByteFileIterator();
  }

  class AvroByteFileIterator extends AbstractBoundedReaderIterator<T> {
    private final ReaderIterator<WindowedValue<ByteBuffer>> avroFileIterator;

    public AvroByteFileIterator() throws IOException {
      avroFileIterator = avroReader.iterator();
    }

    @Override
    protected boolean hasNextImpl() throws IOException {
      return avroFileIterator.hasNext();
    }

    @Override
    protected T nextImpl() throws IOException {
      ByteBuffer inBuffer = avroFileIterator.next().getValue();
      byte[] encodedElem = new byte[inBuffer.remaining()];
      inBuffer.get(encodedElem);
      assert inBuffer.remaining() == 0;
      inBuffer.clear();
      notifyElementRead(encodedElem.length);
      return CoderUtils.decodeFromByteArray(coder, encodedElem);
    }

    @Override
    public void close() throws IOException {
      avroFileIterator.close();
    }

    @Override
    public Progress getProgress() {
      return avroFileIterator.getProgress();
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      return avroFileIterator.requestDynamicSplit(splitRequest);
    }
  }
}

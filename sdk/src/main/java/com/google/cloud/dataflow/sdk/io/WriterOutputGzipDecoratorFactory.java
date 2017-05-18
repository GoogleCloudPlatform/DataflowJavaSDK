/*
 * Copyright (C) 2016 Google Inc.
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

package com.google.cloud.dataflow.sdk.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import com.google.cloud.dataflow.sdk.io.DecoratedFileSink.WriterOutputDecorator;
import com.google.cloud.dataflow.sdk.io.DecoratedFileSink.WriterOutputDecoratorFactory;
import com.google.cloud.dataflow.sdk.util.MimeTypes;

/**
 * Implementation of {@link WriterOutputDecoratorFactory} and {@link WriterOutputDecorator} that provide gzip support
 * via {@link GZIPOutputStream}.
 *
 * @author jeffkpayne
 *
 */
public class WriterOutputGzipDecoratorFactory implements WriterOutputDecoratorFactory {
  private static final WriterOutputGzipDecoratorFactory INSTANCE =
      new WriterOutputGzipDecoratorFactory();

  public static WriterOutputGzipDecoratorFactory getInstance() {
    return INSTANCE;
  }

  private WriterOutputGzipDecoratorFactory() {}

  @Override
  public WriterOutputDecorator create(final OutputStream out) throws IOException {
    return new WriterOutputGzipDecorator(out);
  }

  @Override
  public String getMimeType() {
    return MimeTypes.BINARY;
  }

  private class WriterOutputGzipDecorator extends WriterOutputDecorator {
    private final GZIPOutputStream gzip;

    private WriterOutputGzipDecorator(final OutputStream out) throws IOException {
      super(new GZIPOutputStream(out, true));
      this.gzip = (GZIPOutputStream) super.out;
    }

    @Override
    public void finish() throws IOException {
      gzip.finish();
    }
  }
}

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
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nullable;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.io.FileBasedSink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * The {@link WriterOutputDecoratorFactory} interface and {@link WriterOutputDecorator} abstract
 * class are the hooks that allow for easily customizing how data gets written by
 * {@link DecoratedFileSink} and related classes.
 *
 * @author jeffkpayne
 *
 */
public class DecoratedFileSink<T> extends FileBasedSink<T> {
  private final Coder<T> coder;
  @Nullable
  private final String header;
  @Nullable
  private final String footer;
  private WriterOutputDecoratorFactory writerOutputDecoratorFactory;

  public DecoratedFileSink(final String baseOutputFilename, final String extension,
      final Coder<T> coder, @Nullable final String header, @Nullable final String footer,
      final WriterOutputDecoratorFactory writerOutputDecoratorFactory) {
    super(baseOutputFilename, Preconditions.checkNotNull(extension) + ".gz");
    this.coder = coder;
    this.header = header;
    this.footer = footer;
    this.writerOutputDecoratorFactory = writerOutputDecoratorFactory;
  }

  @Override
  public DecoratedFileWriteOperation<T> createWriteOperation(PipelineOptions options) {
    return new DecoratedFileWriteOperation<>(this, coder, header, footer,
        writerOutputDecoratorFactory);
  }

  @VisibleForTesting
  static class DecoratedFileWriter<T> extends FileBasedWriter<T> {
    private static final byte[] NEWLINE = "\n".getBytes(StandardCharsets.UTF_8);
    private final Coder<T> coder;
    @Nullable
    private final String header;
    @Nullable
    private final String footer;
    private final WriterOutputDecoratorFactory writerOutputDecoratorFactory;
    private WriterOutputDecorator writerOutputDecorator;

    private DecoratedFileWriter(final DecoratedFileWriteOperation<T> writeOperation,
        final Coder<T> coder, @Nullable final String header, @Nullable final String footer,
        final WriterOutputDecoratorFactory writerOutputDecoratorFactory) {
      super(writeOperation);
      this.coder = coder;
      this.header = header;
      this.footer = footer;
      this.writerOutputDecoratorFactory = writerOutputDecoratorFactory;
      this.mimeType = writerOutputDecoratorFactory.getMimeType();
    }

    /**
     * Writes {@code value} followed by a newline if {@code value} is not null.
     */
    private void writeIfNotNull(@Nullable String value) throws IOException {
      if (value != null) {
        synchronized (writerOutputDecorator) {
          writerOutputDecorator.write(value.getBytes(StandardCharsets.UTF_8));
          writerOutputDecorator.write(NEWLINE);
        }
      }
    }

    @Override
    protected void prepareWrite(WritableByteChannel channel) throws Exception {
      writerOutputDecorator =
          writerOutputDecoratorFactory.create(Channels.newOutputStream(channel));
    }

    @Override
    protected void writeHeader() throws Exception {
      writeIfNotNull(header);
    }

    @Override
    protected void writeFooter() throws Exception {
      writeIfNotNull(footer);
      writerOutputDecorator.finish();
    }

    @Override
    public void write(T value) throws Exception {
      synchronized (writerOutputDecorator) {
        coder.encode(value, writerOutputDecorator, Context.OUTER);
        writerOutputDecorator.write(NEWLINE);
      }
    }
  }

  @VisibleForTesting
  static class DecoratedFileWriteOperation<T> extends FileBasedWriteOperation<T> {
    private final Coder<T> coder;
    @Nullable
    private final String header;
    @Nullable
    private final String footer;
    private final WriterOutputDecoratorFactory writerOutputDecoratorFactory;

    /**
     * @param sink
     */
    public DecoratedFileWriteOperation(final DecoratedFileSink<T> sink, final Coder<T> coder,
        @Nullable final String header, @Nullable final String footer,
        final WriterOutputDecoratorFactory writerOutputDecoratorFactory) {
      super(sink);
      this.coder = coder;
      this.header = header;
      this.footer = footer;
      this.writerOutputDecoratorFactory = writerOutputDecoratorFactory;
    }

    /**
     * @see FileBasedWriteOperation#createWriter(PipelineOptions)
     */
    @Override
    public DecoratedFileWriter<T> createWriter(PipelineOptions options) throws Exception {
      return new DecoratedFileWriter<>(this, coder, header, footer, writerOutputDecoratorFactory);
    }
  }

  /**
   * Implementations create instances of {@link WriterOutputDecorator} used by
   * {@link DecoratedFileSink} and related classes to allow <em>decorating</em>, or otherwise
   * transforming, the raw data that would normally be written directly to the {@link OutputStream}
   * passed into {@link WriterOutputDecoratorFactory#create(OutputStream)}.
   *
   * @author jeffkpayne
   *
   */
  public interface WriterOutputDecoratorFactory extends Serializable {
    /**
     * @param out the {@link OutputStream} to write the decorated, or otherwise transformed, data to
     * @return the {@link WriterOutputDecorator} decorating, or otherwise transforming, the raw data
     *         before writing it to <var>out</var>
     * @throws IOException
     */
    public WriterOutputDecorator create(OutputStream out) throws IOException;

    /**
     * @return the MIME type that should be used for the files that will hold the output data
     * @see MimeTypes
     * @see <a href=
     *      'http://www.iana.org/assignments/media-types/media-types.xhtml'>http://www.iana.org/assignments/media-types/media-types.xhtml</a>
     */
    public String getMimeType();
  }

  /**
   * Subclasses of this should <em>decorate</em>, or otherwise transform, the raw data that would
   * normally be written directly to the {@link OutputStream} passed into
   * {@link WriterOutputDecoratorFactory#create(OutputStream)}. Implementations can accomplish this
   * by creating an {@link OutputStream} and passing it into
   * {@link WriterOutputDecorator#WriterOutputDecorator(OutputStream)}.
   * <p>
   * The {@link #finish()} hook is provided for any logic needed to <em>finish</em> writing to the
   * underlying stream without closing it.
   *
   * @author jeffkpayne
   * @see GZIPOutputStream#finish()
   */
  public static abstract class WriterOutputDecorator extends OutputStream {
    protected final OutputStream out;

    public WriterOutputDecorator(final OutputStream out) {
      this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

    /**
     * Provided for any logic needed to <em>finish</em> writing to the {@link WritableByteChannel}
     * without closing the underlying stream. This gets called after any <em>footer</em> data is
     * written by the underlying {@link FileBasedWriter}.
     * <p>
     * Default implementation is a no-op.
     *
     * @throws IOException
     */
    public void finish() throws IOException {};
  }
}

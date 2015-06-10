/*
 * Copyright (C) 2015 The Google Cloud Dataflow Hadoop Library Authors
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

package com.google.cloud.dataflow.contrib.hadoop;

import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.values.KV;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

/**
 * A source that reads from a Hadoop {@link FileInputFormat}.
 *
 * @param <K> the type of the keys read from the source
 * @param <V> the type of the values read from the source
 */
class HadoopReader<K, V> extends Reader<WindowedValue<KV<K, V>>> {

  private final String filepattern;
  private final Class formatClass;

  public HadoopReader(String filepattern, Class formatClass) {
    this.filepattern = filepattern;
    this.formatClass = formatClass;
  }

  @Override
  public ReaderIterator<WindowedValue<KV<K, V>>> iterator() throws IOException {
    return new HadoopReaderIterator();
  }

  class HadoopReaderIterator extends AbstractReaderIterator<WindowedValue<KV<K, V>>> {

    private final FileInputFormat<K, V> format;
    private final TaskAttemptContext attemptContext;
    private final Iterator<InputSplit> splitsIterator;
    private final Configuration conf;
    private RecordReader<K, V> currentReader;
    private boolean hasNext = false;
    private boolean shouldAdvance = true;

    @SuppressWarnings("unchecked")
    public HadoopReaderIterator() throws IOException {
      Job job = Job.getInstance();
      Path path = new Path(filepattern);
      FileInputFormat.addInputPath(job, path);
      FileStatus stat = FileSystem.get(job.getConfiguration()).getFileStatus(path);
      FileInputFormat.setMaxInputSplitSize(job, stat.getLen());

      try {
        this.format = (FileInputFormat<K, V>) formatClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException("Cannot instantiate file input format " + formatClass, e);
      }
      this.attemptContext = new TaskAttemptContextImpl(job.getConfiguration(),
          new TaskAttemptID());

      this.splitsIterator = format.getSplits(job).iterator();
      this.conf = job.getConfiguration();
    }

    @Override
    public boolean hasNext() throws IOException {
      if (shouldAdvance) {
        hasNext = advance();
        shouldAdvance = false;
      }
      return hasNext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public WindowedValue<KV<K, V>> next() throws IOException, NoSuchElementException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      try {
        K key = currentReader.getCurrentKey();
        V value = currentReader.getCurrentValue();
        if (key instanceof Writable) {
          key = (K) WritableUtils.clone((Writable) key, conf);
        }
        if (value instanceof Writable) {
          value = (V) WritableUtils.clone((Writable) value, conf);
        }
        this.shouldAdvance = true;
        return WindowedValue.valueInGlobalWindow(KV.of(key, value));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    private boolean advance() throws IOException {
      try {
        if (currentReader != null && currentReader.nextKeyValue()) {
          return true;
        } else {
          while (splitsIterator.hasNext()) {
            // advance the reader and see if it has records
            InputSplit nextSplit = splitsIterator.next();
            currentReader = format.createRecordReader(nextSplit, attemptContext);
            currentReader.initialize(nextSplit, attemptContext);
            if (currentReader.nextKeyValue()) {
              return true;
            }
          }
          // either no next split or all readers were empty
          return false;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      if (currentReader != null) {
        currentReader.close();
      }
      hasNext = false;
      shouldAdvance = false;
    }
  }
}

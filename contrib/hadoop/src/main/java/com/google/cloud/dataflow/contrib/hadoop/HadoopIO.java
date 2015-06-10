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

import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.ReaderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * {@link PTransform}s for reading and writing Hadoop files.
 *
 * <p> To read a {@link PCollection} from one or more Hadoop files, use
 * {@link HadoopIO.Read}, specifying {@link HadoopIO.Read#from} to specify
 * the path of the file(s) to read from (e.g., a local filename or
 * filename pattern if running locally, or a Hadoop filesystem
 * filename, such as one of the form
 * {@code "hdfs://<namenode>/<path>"}), and optionally
 * {@link HadoopIO.Read#named} to specify the name of the pipeline step.
 *
 * <p> It is required to specify {@link HadoopIO.Read#withFormatClass},
 * {@link HadoopIO.Read#withKeyClass} and {@link HadoopIO.Read#withValueClass}
 * to specify the file input format to use, and the key and value types.
 */
public final class HadoopIO {

  private HadoopIO() {
  }

  /**
   * A root {@link PTransform} that reads from a Hadoop {@link FileInputFormat} and
   * returns a {@link PCollection} containing the decoding of each key-value pair.
   */
  public static final class Read {

    private Read() {
    }

    /**
     * Returns an HadoopIO.Read PTransform with the given step name.
     */
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * Returns an HadoopIO.Read PTransform that reads from the file(s)
     * with the given name or pattern.  This can be a local filename
     * or filename pattern (if running locally), or a Hadoop filesystem filename, such
     * as one of the form {@code "hdfs://<namenode>/<path>"})
     * (if running locally or via the Spark Dataflow service).
     */
    public static Bound from(String filepattern) {
      return new Bound().from(filepattern);
    }

    /**
     * Returns an HadoopIO.Read PTransform that reads Hadoop file(s) using the
     * specified file input format.
     */
    @SuppressWarnings("unchecked")
    public static Bound withFormatClass(Class<? extends FileInputFormat<?, ?>> format) {
      return new Bound().withFormatClass(format);
    }

    /**
     * Returns an HadoopIO.Read PTransform that reads Hadoop file(s) containing records
     * with the specified key type.
     */
    @SuppressWarnings("unchecked")
    public static Bound withKeyClass(Class<?> key) {
      return new Bound().withKeyClass(key);
    }

    /**
     * Returns an HadoopIO.Read PTransform that reads Hadoop file(s) containing records
     * with the specified value type.
     */
    @SuppressWarnings("unchecked")
    public static Bound withValueClass(Class<?> value) {
      return new Bound().withValueClass(value);
    }

    /**
     * A {@link PTransform} that reads from a Hadoop {@link FileInputFormat} and
     * returns a {@link PCollection} containing the decoding of each key-value pair.
     *
     * @param <K> the type of each of the keys of the resulting PCollection
     * @param <V> the type of each of the values of the resulting PCollection
     */
    public static class Bound<K, V> extends PTransform<PInput, PCollection<KV<K, V>>> {

      private final String filepattern;
      private final Class<? extends FileInputFormat<K, V>> formatClass;
      private final Class<K> keyClass;
      private final Class<V> valueClass;

      Bound() {
        this(null, null, null, null, null);
      }

      Bound(String name, String filepattern, Class<? extends FileInputFormat<K, V>> format,
          Class<K> key, Class<V> value) {
        super(name);
        this.filepattern = filepattern;
        this.formatClass = format;
        this.keyClass = key;
        this.valueClass = value;
      }

      /**
       * Returns a new HadoopIO.Read PTransform that's like this one but
       * with the given step name.  Does not modify this object.
       */
      public Bound<K, V> named(String name) {
        return new Bound<>(name, filepattern, formatClass, keyClass, valueClass);
      }

      /**
       * Returns a new Hadoop.Read PTransform that's like this one but
       * that reads from the file(s) with the given name or pattern.
       * Does not modify this object.
       */
      public Bound<K, V> from(String file) {
        return new Bound<>(name, file, formatClass, keyClass, valueClass);
      }

      /**
       * Returns a new Hadoop.Read PTransform that's like this one but
       * that reads from file(s) using the given file input format.
       * Does not modify this object.
       */
      public Bound<K, V> withFormatClass(Class<? extends FileInputFormat<K, V>> format) {
        return new Bound<>(name, filepattern, format, keyClass, valueClass);
      }

      /**
       * Returns a new Hadoop.Read PTransform that's like this one but
       * that reads from file(s) containing records with the specified key type.
       * Does not modify this object.
       */
      public Bound<K, V> withKeyClass(Class<K> key) {
        return new Bound<>(name, filepattern, formatClass, key, valueClass);
      }

      /**
       * Returns a new Hadoop.Read PTransform that's like this one but
       * that reads from file(s) containing records with the specified value type.
       * Does not modify this object.
       */
      public Bound<K, V> withValueClass(Class<V> value) {
        return new Bound<>(name, filepattern, formatClass, keyClass, value);
      }

      public String getFilepattern() {
        return filepattern;
      }

      public Class<? extends FileInputFormat<K, V>> getFormatClass() {
        return formatClass;
      }

      public Class<K> getKeyClass() {
        return keyClass;
      }

      public Class<V> getValueClass() {
        return valueClass;
      }

      @Override
      public PCollection<KV<K, V>> apply(PInput input) {
        Preconditions.checkNotNull(filepattern,
            "need to set the filepattern of an HadoopIO.Read transform");
        Preconditions.checkNotNull(formatClass,
            "need to set the format class of an HadoopIO.Read transform");
        Preconditions.checkNotNull(keyClass,
            "need to set the key class of an HadoopIO.Read transform");
        Preconditions.checkNotNull(valueClass,
            "need to set the value class of an HadoopIO.Read transform");

        return PCollection.createPrimitiveOutputInternal(input.getPipeline(),
            WindowingStrategy.globalDefault(), PCollection.IsBounded.BOUNDED);
      }

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform, DirectPipelineRunner.EvaluationContext context) {
                evaluateReadHelper(transform, context);
              }
            });
      }

    }

  }

  private static <K, V> void evaluateReadHelper(
      Read.Bound<K, V> transform, DirectPipelineRunner.EvaluationContext context) {
    HadoopReader<K, V> reader = new HadoopReader<>(transform.filepattern,
        transform.formatClass);
    List<WindowedValue<KV<K, V>>> elems = ReaderUtils.readElemsFromReader(reader);
    List<DirectPipelineRunner.ValueWithMetadata<KV<K, V>>> output = new ArrayList<>();
    for (WindowedValue<KV<K, V>> elem : elems) {
      output.add(DirectPipelineRunner.ValueWithMetadata.of(elem));
    }
    context.setPCollectionValuesWithMetadata(context.getOutput(transform), output);
  }
}

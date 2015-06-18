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

package com.google.cloud.dataflow.sdk.io;

import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.worker.BigQueryReader;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterFirst;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.BigQueryTableInserter;
import com.google.cloud.dataflow.sdk.util.ReaderUtils;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link PTransform}s for reading and writing
 * <a href="https://developers.google.com/bigquery/">BigQuery</a> tables.
 * <p><h3>Table References</h3>
 * A fully-qualified BigQuery table name consists of three components:
 * <ul>
 *   <li>{@code projectId}: the Cloud project id (defaults to
 *       {@link GcpOptions#getProject()}).
 *   <li>{@code datasetId}: the BigQuery dataset id, unique within a project.
 *   <li>{@code tableId}: a table id, unique within a dataset.
 * </ul>
 * <p>
 * BigQuery table references are stored as a {@link TableReference}, which comes
 * from the <a href="https://cloud.google.com/bigquery/client-libraries">
 * BigQuery Java Client API</a>.
 * Tables can be referred to as Strings, with or without the {@code projectId}.
 * A helper function is provided ({@link BigQueryIO#parseTableSpec(String)})
 * that parses the following string forms into a {@link TableReference}:
 * <ul>
 *   <li>[{@code project_id}]:[{@code dataset_id}].[{@code table_id}]
 *   <li>[{@code dataset_id}].[{@code table_id}]
 * </ul>
 * <p><h3>Reading</h3>
 * To read from a BigQuery table, apply a {@link BigQueryIO.Read} transformation.
 * This produces a {@code PCollection<TableRow>} as output:
 * <pre>{@code
 * PCollection<TableRow> shakespeare = pipeline.apply(
 *     BigQueryIO.Read
 *         .named("Read")
 *         .from("clouddataflow-readonly:samples.weather_stations");
 * }</pre>
 * <p><h3>Writing</h3>
 * To write to a BigQuery table, apply a {@link BigQueryIO.Write} transformation.
 * This consumes a {@code PCollection<TableRow>} as input.
 * <p>
 * <pre>{@code
 * PCollection<TableRow> quotes = ...
 *
 * List<TableFieldSchema> fields = new ArrayList<>();
 * fields.add(new TableFieldSchema().setName("source").setType("STRING"));
 * fields.add(new TableFieldSchema().setName("quote").setType("STRING"));
 * TableSchema schema = new TableSchema().setFields(fields);
 *
 * quotes.apply(BigQueryIO.Write
 *     .named("Write")
 *     .to("my-project:output.output_table")
 *     .withSchema(schema)
 *     .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
 * }</pre>
 * <p>
 * See {@link BigQueryIO.Write} for details on how to specify if a write should
 * append to an existing table, replace the table, or verify that the table is
 * empty. Note that the dataset being written to must already exist.
 *
 * @see <a href="https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableRow.html">TableRow</a>
 */
public class BigQueryIO {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryIO.class);

  /**
   * Singleton instance of the JSON factory used to read and write JSON
   * formatted rows.
   */
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes.
   * IDs must start with a letter and may not end with a dash.
   * This regex isn't exact - this allows for patterns that would be rejected by
   * the service, but this is sufficient for basic parsing of table references.
   */
  private static final String PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]";

  /**
   * Regular expression that matches Dataset IDs.
   */
  private static final String DATASET_REGEXP = "[-\\w.]{1,1024}";

  /**
   * Regular expression that matches Table IDs.
   */
  private static final String TABLE_REGEXP = "[-\\w$@]{1,1024}";

  /**
   * Matches table specifications in the form
   * "[project_id]:[dataset_id].[table_id]" or "[dataset_id].[table_id]".
   */
  private static final String DATASET_TABLE_REGEXP =
      String.format("((?<PROJECT>%s):)?(?<DATASET>%s)\\.(?<TABLE>%s)", PROJECT_ID_REGEXP,
          DATASET_REGEXP, TABLE_REGEXP);

  private static final Pattern TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP);

  /**
   * Parse a table specification in the form
   * "[project_id]:[dataset_id].[table_id]" or "[dataset_id].[table_id]".
   * <p>
   * If the project id is omitted, the default project id is used.
   */
  public static TableReference parseTableSpec(String tableSpec) {
    Matcher match = TABLE_SPEC.matcher(tableSpec);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Table reference is not in [project_id]:[dataset_id].[table_id] "
          + "format: " + tableSpec);
    }

    TableReference ref = new TableReference();
    ref.setProjectId(match.group("PROJECT"));

    return ref.setDatasetId(match.group("DATASET")).setTableId(match.group("TABLE"));
  }

  /**
   * Returns a canonical string representation of the TableReference.
   */
  public static String toTableSpec(TableReference ref) {
    StringBuilder sb = new StringBuilder();
    if (ref.getProjectId() != null) {
      sb.append(ref.getProjectId());
      sb.append(":");
    }

    sb.append(ref.getDatasetId()).append('.').append(ref.getTableId());
    return sb.toString();
  }

  /**
   * A {@link PTransform} that reads from a BigQuery table and returns a
   * {@link PCollection} of {@link TableRow TableRows} containing each of the rows of the table.
   * <p>
   * Each TableRow record contains values indexed by column name.  Here is a
   * sample processing function that processes a "line" column from rows:
   * <pre><code>
   * static class ExtractWordsFn extends DoFn{@literal <TableRow, String>} {
   *   {@literal @}Override
   *   public void processElement(ProcessContext c) {
   *     // Get the "line" field of the TableRow object, split it into words, and emit them.
   *     TableRow row = c.element();
   *     String[] words = row.get("line").toString().split("[^a-zA-Z']+");
   *     for (String word : words) {
   *       if (!word.isEmpty()) {
   *         c.output(word);
   *       }
   *     }
   *   }
   * }
   * </code></pre>
   */
  public static class Read {
    public static Bound<TableRow> named(String name) {
      return new Bound(TableRow.class).named(name);
    }

    /**
     * Reads a BigQuery table specified as
     * "[project_id]:[dataset_id].[table_id]" or "[dataset_id].[table_id]" for
     * tables within the current project.
     */
    public static Bound<TableRow> from(String tableSpec) {
      return new Bound(TableRow.class).from(tableSpec);
    }

    /**
     * Reads a BigQuery table specified as a TableReference object.
     */
    public static Bound<TableRow> from(TableReference table) {
      return new Bound(TableRow.class).from(table);
    }

    /**
     * Disables BigQuery table validation, which is enabled by default.
     */
    public static Bound<TableRow> withoutValidation() {
      return new Bound(TableRow.class).withoutValidation();
    }

    /**
     * Use typing.
     * @param type
     * @param <T>
     * @return
     */
    public static <T> Bound<T> withType(Class<T> type) {
      return new Bound<>(type);
    }

    /**
     * A {@link PTransform} that reads from a BigQuery table and returns a bounded
     * {@link PCollection} of {@link TableRow TableRows}.
     */
    public static class Bound<T> extends PTransform<PInput, PCollection<T>> {
      private static final long serialVersionUID = 0;

      TableReference table;
      final boolean validate;
      /** The class type of the row. */
      final Class<T> type;

      Bound(Class<T> type) {
        this.type = type;
        this.validate = true;
      }

      Bound(String name, TableReference reference, Class<T> type, boolean validate) {
        super(name);
        this.table = reference;
        this.type = type;
        this.validate = validate;
      }

      /**
       * Sets the name associated with this transformation.
       */
      public Bound<T> named(String name) {
        return new Bound(name, table, type, validate);
      }

      /**
       * Sets the table specification.
       * <p>
       * Refer to {@link #parseTableSpec(String)} for the specification format.
       */
      public Bound<T> from(String tableSpec) {
        return from(parseTableSpec(tableSpec));
      }

      /**
       * Sets the table specification.
       */
      public Bound<T> from(TableReference table) {
        return new Bound(name, table, type, validate);
      }

      /**
       * Disable table validation.
       */
      public Bound<T> withoutValidation() {
        return new Bound(name, table, type, false);
      }

      @Override
      public PCollection<T> apply(PInput input) {
        if (table == null) {
          throw new IllegalStateException(
              "must set the table reference of a BigQueryIO.Read transform");
        }
        if (TableRow.class.equals(type)) {
          return (PCollection<T>) PCollection.<TableRow>createPrimitiveOutputInternal(
            input.getPipeline(),
            WindowingStrategy.globalDefault(),
            IsBounded.BOUNDED)
            // Force the output's Coder to be what the read is using, and
            // unchangeable later, to ensure that we read the input in the
            // format specified by the Read transform.
            .setCoder(TableRowJsonCoder.of());
        }
        PCollection<T> pCollection = (PCollection<T>) PCollection.
          <TableRow>createPrimitiveOutputInternal(
            input.getPipeline(),
            WindowingStrategy.globalDefault(),
            IsBounded.BOUNDED);

        if (Serializable.class.isAssignableFrom(type)) {
          try {
            pCollection.setCoder((Coder<T>) SerializableCoder.of(type.getName()));
          } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
              "typed BigQueryIO.Read should be serializable",
              e);
          }
        } else {
          throw new IllegalStateException(
            "type show be serializable");
        }
        return pCollection;
      }

      @Override
      protected Coder<T> getDefaultOutputCoder() {
        if (TableRow.class.equals(type)) {
          return (Coder<T>) TableRowJsonCoder.of();
        }
        try {
          return (Coder<T>) SerializableCoder.of(type.getName());
        } catch (ClassNotFoundException e) {
          throw new IllegalStateException(
            "typed BigQueryIO.Read should be serializable",
            e);
        }
      }

      @Override
      protected String getKindString() {
        return "BigQueryIO.Read";
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

      /**
       * Returns the table to write.
       */
      public TableReference getTable() {
        return table;
      }

      /**
       * Returns true if table validation is enabled.
       */
      public boolean getValidate() {
        return validate;
      }

      /**
       * Return the type of model object.
       */
      public Class<T> getType() {
        return type;
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link PTransform} that writes a {@link PCollection} containing {@link TableRow TableRows}
   * to a BigQuery table.
   * <p>
   * By default, tables will be created if they do not exist, which
   * corresponds to a {@code CreateDisposition.CREATE_IF_NEEDED} disposition
   * that matches the default of BigQuery's Jobs API.  A schema must be
   * provided (via {@link Write#withSchema}), or else the transform may fail
   * at runtime with an {@link java.lang.IllegalArgumentException}.
   * <p>
   * The dataset being written must already exist.
   * <p>
   * By default, writes require an empty table, which corresponds to
   * a {@code WriteDisposition.WRITE_EMPTY} disposition that matches the
   * default of BigQuery's Jobs API.
   * <p>
   * Here is a sample transform that produces TableRow values containing
   * "word" and "count" columns:
   * <pre><code>
   * static class FormatCountsFn extends DoFnP{@literal <KV<String, Long>, TableRow>} {
   *   {@literal @}Override
   *   public void processElement(ProcessContext c) {
   *     TableRow row = new TableRow()
   *         .set("word", c.element().getKey())
   *         .set("count", c.element().getValue().intValue());
   *     c.output(row);
   *   }
   * }
   * </code></pre>
   */
  public static class Write {
    /**
     * An enumeration type for the BigQuery create disposition strings publicly
     * documented as {@code CREATE_NEVER}, and {@code CREATE_IF_NEEDED}.
     */
    public enum CreateDisposition {
      /**
       * Specifics that tables should not be created.
       * <p>
       * If the output table does not exist, the write fails.
       */
      CREATE_NEVER,

      /**
       * Specifies that tables should be created if needed. This is the default
       * behavior.
       * <p>
       * Requires that a table schema is provided via {@link Write#withSchema}.
       * This precondition is checked before starting a job. The schema is
       * not required to match an existing table's schema.
       * <p>
       * When this transformation is executed, if the output table does not
       * exist, the table is created from the provided schema. Note that even if
       * the table exists, it may be recreated if necessary when paired with a
       * {@link WriteDisposition#WRITE_TRUNCATE}.
       */
      CREATE_IF_NEEDED
    }

    /**
     * An enumeration type for the BigQuery write disposition strings publicly
     * documented as {@code WRITE_TRUNCATE}, {@code WRITE_APPEND}, and
     * {@code WRITE_EMPTY}.
     */
    public enum WriteDisposition {
      /**
       * Specifies that write should replace a table.
       * <p>
       * The replacement may occur in multiple steps - for instance by first
       * removing the existing table, then creating a replacement, then filling
       * it in.  This is not an atomic operation, and external programs may
       * see the table in any of these intermediate steps.
       */
      WRITE_TRUNCATE,

      /**
       * Specifies that rows may be appended to an existing table.
       */
      WRITE_APPEND,

      /**
       * Specifies that the output table must be empty. This is the default
       * behavior.
       * <p>
       * If the output table is not empty, the write fails at runtime.
       * <p>
       * This check may occur long before data is written, and does not
       * guarantee exclusive access to the table.  If two programs are run
       * concurrently, each specifying the same output table and
       * a {@link WriteDisposition} of {@code WRITE_EMPTY}, it is possible
       * for both to succeed.
       */
      WRITE_EMPTY
    }

    /**
     * Sets the name associated with this transformation.
     */
    public static Bound named(String name) {
      return new Bound().named(name);
    }

    /**
     * Creates a write transformation for the given table specification.
     * <p>
     * Refer to {@link #parseTableSpec(String)} for the specification format.
     */
    public static Bound to(String tableSpec) {
      return new Bound().to(tableSpec);
    }

    /** Creates a write transformation for the given table. */
    public static Bound to(TableReference table) {
      return new Bound().to(table);
    }

    /**
     * Specifies a table schema to use in table creation.
     * <p>
     * The schema is required only if writing to a table that does not already
     * exist, and {@link BigQueryIO.Write.CreateDisposition} is set to
     * {@code CREATE_IF_NEEDED}.
     */
    public static Bound withSchema(TableSchema schema) {
      return new Bound().withSchema(schema);
    }

    /** Specifies options for creating the table. */
    public static Bound withCreateDisposition(CreateDisposition disposition) {
      return new Bound().withCreateDisposition(disposition);
    }

    /** Specifies options for writing to the table. */
    public static Bound withWriteDisposition(WriteDisposition disposition) {
      return new Bound().withWriteDisposition(disposition);
    }

    /**
     * Disables BigQuery table validation, which is enabled by default.
     */
    public static Bound withoutValidation() {
      return new Bound().withoutValidation();
    }

    /**
     * A {@link PTransform} that can write either a bounded or unbounded
     * {@link PCollection} of {@link TableRow TableRows} to a BigQuery table.
     */
    public static class Bound extends PTransform<PCollection<TableRow>, PDone> {
      private static final long serialVersionUID = 0;

      final TableReference table;

      // Table schema. The schema is required only if the table does not exist.
      final TableSchema schema;

      // Options for creating the table. Valid values are CREATE_IF_NEEDED and
      // CREATE_NEVER.
      final CreateDisposition createDisposition;

      // Options for writing to the table. Valid values are WRITE_TRUNCATE,
      // WRITE_APPEND and WRITE_EMPTY.
      final WriteDisposition writeDisposition;

      // An option to indicate if table validation is desired. Default is true.
      final boolean validate;

      public Bound() {
        this.table = null;
        this.schema = null;
        this.createDisposition = CreateDisposition.CREATE_IF_NEEDED;
        this.writeDisposition = WriteDisposition.WRITE_EMPTY;
        this.validate = true;
      }

      Bound(String name, TableReference ref, TableSchema schema,
          CreateDisposition createDisposition, WriteDisposition writeDisposition,
          boolean validate) {
        super(name);
        this.table = ref;
        this.schema = schema;
        this.createDisposition = createDisposition;
        this.writeDisposition = writeDisposition;
        this.validate = validate;
      }

      /**
       * Sets the name associated with this transformation.
       */
      public Bound named(String name) {
        return new Bound(name, table, schema, createDisposition, writeDisposition, validate);
      }

      /**
       * Specifies the table specification.
       * <p>
       * Refer to {@link #parseTableSpec(String)} for the specification format.
       */
      public Bound to(String tableSpec) {
        return to(parseTableSpec(tableSpec));
      }

      /**
       * Specifies the table to be written to.
       */
      public Bound to(TableReference table) {
        return new Bound(name, table, schema, createDisposition, writeDisposition, validate);
      }

      /**
       * Specifies the table schema, used if the table is created.
       */
      public Bound withSchema(TableSchema schema) {
        return new Bound(name, table, schema, createDisposition, writeDisposition, validate);
      }

      /** Specifies options for creating the table. */
      public Bound withCreateDisposition(CreateDisposition createDisposition) {
        return new Bound(name, table, schema, createDisposition, writeDisposition, validate);
      }

      /** Specifies options for writing the table. */
      public Bound withWriteDisposition(WriteDisposition writeDisposition) {
        return new Bound(name, table, schema, createDisposition, writeDisposition, validate);
      }

      /**
       * Disable table validation.
       */
      public Bound withoutValidation() {
        return new Bound(name, table, schema, createDisposition, writeDisposition, false);
      }

      @Override
      public PDone apply(PCollection<TableRow> input) {
        if (table == null) {
          throw new IllegalStateException(
              "must set the table reference of a BigQueryIO.Write transform");
        }

        if (createDisposition == CreateDisposition.CREATE_IF_NEEDED && schema == null) {
          throw new IllegalArgumentException("CreateDisposition is CREATE_IF_NEEDED, "
              + "however no schema was provided.");
        }

        // In streaming, BigQuery write is taken care of by StreamWithDeDup transform.
        BigQueryOptions options = input.getPipeline().getOptions().as(BigQueryOptions.class);
        if (options.isStreaming()) {
          return input.apply(new StreamWithDeDup(table, schema));
        }

        return PDone.in(input.getPipeline());
      }

      @Override
      protected Coder<Void> getDefaultOutputCoder() {
        return VoidCoder.of();
      }

      @Override
      protected String getKindString() {
        return "BigQueryIO.Write";
      }

      static {
        DirectPipelineRunner.registerDefaultTransformEvaluator(
            Bound.class, new DirectPipelineRunner.TransformEvaluator<Bound>() {
              @Override
              public void evaluate(
                  Bound transform, DirectPipelineRunner.EvaluationContext context) {
                evaluateWriteHelper(transform, context);
              }
            });
      }

      /** Returns the create disposition. */
      public CreateDisposition getCreateDisposition() {
        return createDisposition;
      }

      /** Returns the write disposition. */
      public WriteDisposition getWriteDisposition() {
        return writeDisposition;
      }

      /** Returns the table schema. */
      public TableSchema getSchema() {
        return schema;
      }

      /** Returns the table reference. */
      public TableReference getTable() {
        return table;
      }

      /** Returns true if table validation is enabled. */
      public boolean getValidate() {
        return validate;
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Implementation of DoFn to perform streaming BigQuery write.
   */
  private static class StreamingWriteFn
      extends DoFn<KV<String, TableRow>, Void> {
    private static final long serialVersionUID = 0;

    /** TableReference in JSON.  Use String to make the class Serializable. */
    private final String jsonTableReference;

    /** TableSchema in JSON.  Use String to make the class Serializable. */
    private final String jsonTableSchema;

    private transient TableReference tableReference;

    /** JsonTableRows to accumulate BigQuery rows. */
    private transient List<TableRow> tableRows;

    /** The list of unique ids for each BigQuery table row. */
    private transient List<String> uniqueIdsForTableRows;

    /** The list of tables created so far, so we don't try the creation
        each time. */
    private static Set<String> createdTables =
        Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /** Constructor. */
    StreamingWriteFn(TableReference table, TableSchema schema) {
      try {
        jsonTableReference = JSON_FACTORY.toString(table);
        jsonTableSchema = JSON_FACTORY.toString(schema);
      } catch (IOException e) {
        throw new RuntimeException("Cannot initialize BigQuery streaming writer.", e);
      }
    }

    /** Prepares a target BigQuery table. */
    @Override
    public void startBundle(Context context) {
      tableRows = new ArrayList<>();
      uniqueIdsForTableRows = new ArrayList<>();
      BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);

      // TODO: Support table sharding and the better place to initialize
      // BigQuery table.
      try {
        tableReference =
            JSON_FACTORY.fromString(jsonTableReference, TableReference.class);
        String tableSpec = toTableSpec(tableReference);

        if (!createdTables.contains(tableSpec)) {
          synchronized (createdTables) {
            // Another thread may have succeeded in creating the table in the meanwhile, so
            // check again. This check isn't needed for correctness, but we add it to prevent
            // every thread from attempting a create and overwhelming our BigQuery quota.
            if (!createdTables.contains(tableSpec)) {
              TableSchema tableSchema = JSON_FACTORY.fromString(jsonTableSchema, TableSchema.class);
              Bigquery client = Transport.newBigQueryClient(options).build();
              BigQueryTableInserter inserter = new BigQueryTableInserter(client, tableReference);
              inserter.tryCreateTable(tableSchema);
              createdTables.add(tableSpec);
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
    @Override
    public void processElement(ProcessContext context) {
      KV<String, TableRow> kv = context.element();
      addRow(kv.getValue(), kv.getKey());
    }

    /** Writes the accumulated rows into BigQuery with streaming API. */
    @Override
    public void finishBundle(Context context) {
      flushRows(context.getPipelineOptions().as(BigQueryOptions.class));
    }

    /** Accumulate a row to be written to BigQuery. */
    private void addRow(TableRow tableRow, String uniqueId) {
      uniqueIdsForTableRows.add(uniqueId);
      tableRows.add(tableRow);
    }

    /** Writes the accumulated rows into BigQuery with streaming API. */
    private void flushRows(BigQueryOptions options) {
      if (!tableRows.isEmpty()) {
        Bigquery client = Transport.newBigQueryClient(options).build();
        try {
          BigQueryTableInserter inserter = new BigQueryTableInserter(client, tableReference);
          inserter.insertAll(tableRows, uniqueIdsForTableRows);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        tableRows.clear();
        uniqueIdsForTableRows.clear();
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Fn that tags each table row with a unique id.
   * To avoid calling UUID.randomUUID() for each element, which can be costly,
   * a randomUUID is generated only once per bucket of data. The actual unique
   * id is created by concatenating this randomUUID with a sequential number.
   */
  private static class TagWithUniqueIds extends DoFn<TableRow, KV<Integer, KV<String, TableRow>>> {
    private static final long serialVersionUID = 0;

    private transient String randomUUID;
    private transient AtomicLong sequenceNo;

    @Override
    public void startBundle(Context context) {
      randomUUID = UUID.randomUUID().toString();
      sequenceNo = new AtomicLong();
    }

    /** Tag the input with a unique id. */
    @Override
    public void processElement(ProcessContext context) {
      String uniqueId = randomUUID + Long.toString(sequenceNo.getAndIncrement());
      ThreadLocalRandom randomGenerator = ThreadLocalRandom.current();
      // We output on keys 0-50 to ensure that there's enough batching for
      // BigQuery.
      context.output(KV.of(randomGenerator.nextInt(0, 50), KV.of(uniqueId, context.element())));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
  * PTransform that performs streaming BigQuery write. To increase consistency,
  * it leverages BigQuery best effort de-dup mechanism.
   */
  private static class StreamWithDeDup extends PTransform<PCollection<TableRow>, PDone> {
    private static final long serialVersionUID = 0;

    // TODO: Consider making these configurable.
    private static final int WRITE_BUFFER_COUNT = 100;
    private static final Duration WRITE_BUFFER_WAIT = Duration.standardSeconds(1);

    private final TableReference tableReference;
    private final TableSchema tableSchema;

    /** Constructor. */
    StreamWithDeDup(TableReference tableReference, TableSchema tableSchema) {
      this.tableReference = tableReference;
      this.tableSchema = tableSchema;
    }

    @Override
    protected Coder<Void> getDefaultOutputCoder() {
      return VoidCoder.of();
    }

    @Override
    public PDone apply(PCollection<TableRow> input) {
      // A naive implementation would be to simply stream data directly to BigQuery.
      // However, this could occasionally lead to duplicated data, e.g., when
      // a VM that runs this code is restarted and the code is re-run.

      // The above risk is mitigated in this implementation by relying on
      // BigQuery built-in best effort de-dup mechanism.

      // To use this mechanism, each input TableRow is tagged with a generated
      // unique id, which is then passed to BigQuery and used to ignore duplicates.

      PCollection<KV<Integer, KV<String, TableRow>>> tagged =
          input.apply(ParDo.of(new TagWithUniqueIds()));

      // To prevent having the same TableRow processed more than once with regenerated
      // different unique ids, this implementation relies on "checkpointing", which is
      // achieved as a side effect of having StreamingWriteFn immediately follow a GBK.
      tagged
          .apply(Window.<KV<Integer, KV<String, TableRow>>>into(new GlobalWindows())
                       .triggering(Repeatedly.forever(
                           AfterFirst.of(
                               AfterProcessingTime.pastFirstElementInPane()
                                                  .plusDelayOf(WRITE_BUFFER_WAIT),
                               AfterPane.elementCountAtLeast(WRITE_BUFFER_COUNT))))
                       .discardingFiredPanes())
          .apply(GroupByKey.<Integer, KV<String, TableRow>>create())
          .apply(Values.<Iterable<KV<String, TableRow>>>create())
          .apply(Flatten.<KV<String, TableRow>>iterables())
          .apply(ParDo.of(new StreamingWriteFn(tableReference, tableSchema)));

      // Note that the implementation to return PDone here breaks the
      // implicit assumption about the job execution order.  If a user
      // implements a PTransform that takes PDone returned here as its
      // input, the transform may not necessarily be executed after
      // the BigQueryIO.Write.

      return PDone.in(input.getPipeline());
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * Direct mode read evaluator.
   * <p>
   * This loads the entire table into an in-memory PCollection.
   */
  private static <T> void evaluateReadHelper(
      Read.Bound<T> transform, DirectPipelineRunner.EvaluationContext context) {
    BigQueryOptions options = context.getPipelineOptions();
    Bigquery client = Transport.newBigQueryClient(options).build();
    TableReference ref = transform.table;
    if (ref.getProjectId() == null) {
      ref.setProjectId(options.getProject());
    }

    LOG.info("Reading from BigQuery table {}", toTableSpec(ref));
    List<WindowedValue<T>> elems = ReaderUtils.readElemsFromReader(
      new BigQueryReader(client, ref, transform.getType()));
    LOG.info("Number of records read from BigQuery: {}", elems.size());
    context.setPCollectionWindowedValue((PCollection) context.getOutput(transform), elems);
  }

  /**
   * Direct mode write evaluator.
   * <p>
   * This writes the entire table in a single BigQuery request.
   * The table will be created if necessary.
   */
  private static void evaluateWriteHelper(
      Write.Bound transform, DirectPipelineRunner.EvaluationContext context) {
    BigQueryOptions options = context.getPipelineOptions();
    Bigquery client = Transport.newBigQueryClient(options).build();
    TableReference ref = transform.table;
    if (ref.getProjectId() == null) {
      ref.setProjectId(options.getProject());
    }

    LOG.info("Writing to BigQuery table {}", toTableSpec(ref));

    try {
      BigQueryTableInserter inserter = new BigQueryTableInserter(client, ref);

      inserter.getOrCreateTable(
          transform.writeDisposition, transform.createDisposition, transform.schema);

      List<TableRow> tableRows = context.getPCollection(context.getInput(transform));
      inserter.insertAll(tableRows);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

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

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Iterates over all rows in a table.
 *
 * @param <T> the type of the elements read from the source
 */
public abstract class AbstractBigQueryIterator<T> implements Iterator<T>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableRowIterator.class);

  private final Bigquery client;
  private final TableReference ref;
  protected TableSchema schema;
  private String pageToken;
  private Iterator<TableRow> rowIterator;
  // Set true when the final page is seen from the service.
  private boolean lastPage = false;

  // The maximum number of times a BigQuery request will be retried
  private static final int MAX_RETRIES = 3;
  // Initial wait time for the backoff implementation
  private static final int INITIAL_BACKOFF_MILLIS = 1000;


  public AbstractBigQueryIterator(Bigquery client, TableReference ref) {
    this.client = client;
    this.ref = ref;
  }

  @Override
  public boolean hasNext() {
    try {
      if (!isOpen()) {
        open();
      }

      if (!rowIterator.hasNext() && !lastPage) {
        readNext();
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    return rowIterator.hasNext();
  }

  protected abstract T getTypedTableRow(List<TableFieldSchema> fields, Map<String, Object> rawRow);

  protected abstract void buildMapper();

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // Embed schema information into the raw row, so that values have an
    // associated key.  This matches how rows are read when using the
    // DataflowPipelineRunner.
    return getTypedTableRow(schema.getFields(), rowIterator.next());
  }


  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private void readNext() throws IOException, InterruptedException {
    Bigquery.Tabledata.List list = client.tabledata()
      .list(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());
    if (pageToken != null) {
      list.setPageToken(pageToken);
    }

    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff = new AttemptBoundedExponentialBackOff(MAX_RETRIES, INITIAL_BACKOFF_MILLIS);

    TableDataList result = null;
    while (true) {
      try {
        result = list.execute();
        break;
      } catch (IOException e) {
        LOG.error("Error reading from BigQuery table {} of dataset {} : {}", ref.getTableId(),
          ref.getDatasetId(), e.getMessage());
        if (!BackOffUtils.next(sleeper, backOff)) {
          LOG.error("Aborting after {} retries.", MAX_RETRIES);
          throw e;
        }
      }
    }

    pageToken = result.getPageToken();
    rowIterator = result.getRows() != null ? result.getRows().iterator() :
      Collections.<TableRow>emptyIterator();

    // The server may return a page token indefinitely on a zero-length table.
    if (pageToken == null ||
      result.getTotalRows() != null && result.getTotalRows() == 0) {
      lastPage = true;
    }
  }

  @Override
  public void close() throws IOException {
    // Prevent any further requests.
    lastPage = true;
  }

  private boolean isOpen() {
    return schema != null;
  }

  /**
   * Opens the table for read.
   * @throws IOException on failure
   */
  private void open() throws IOException, InterruptedException {
    // Get table schema.
    Bigquery.Tables.Get get = client.tables()
      .get(ref.getProjectId(), ref.getDatasetId(), ref.getTableId());

    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff = new AttemptBoundedExponentialBackOff(MAX_RETRIES, INITIAL_BACKOFF_MILLIS);
    Table table = null;

    while (true) {
      try {
        table = get.execute();
        break;
      } catch (IOException e) {
        LOG.error("Error opening BigQuery table {} of dataset {} : {}", ref.getTableId(),
          ref.getDatasetId(), e.getMessage());
        if (!BackOffUtils.next(sleeper, backOff)) {
          LOG.error("Aborting after {} retries.", MAX_RETRIES);
          throw e;
        }
      }
    }

    schema = table.getSchema();
    buildMapper();

    // Read the first page of results.
    readNext();
  }
}

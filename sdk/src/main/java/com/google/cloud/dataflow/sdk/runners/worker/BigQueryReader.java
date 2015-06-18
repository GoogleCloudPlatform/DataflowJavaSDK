/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.util.AbstractBigQueryIterator;
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator;
import com.google.cloud.dataflow.sdk.util.BigQueryTypedIterator;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * A source that reads a BigQuery table and yields TableRow objects.
 *
 * <p>The source is a wrapper over the {@code BigQueryTableRowIterator} class, which issues a
 * query for all rows of a table and then iterates over the result. There is no support for
 * progress reporting because the source is used only in situations where the entire table must be
 * read by each worker (i.e. the source is used as a side input).
 *
 * @param <T> the type of the elements read from the source
 */
public class BigQueryReader<T> extends Reader<WindowedValue<T>> {
  final TableReference tableRef;
  final BigQueryOptions bigQueryOptions;
  final Bigquery bigQueryClient;
  final Class<T> type;

  /** Builds a BigQuery source using pipeline options to instantiate a Bigquery client. */
  public BigQueryReader(BigQueryOptions bigQueryOptions, TableReference tableRef, Class<T> type) {
    // Save pipeline options so that we can construct the BigQuery client on-demand whenever an
    // iterator gets created.
    this.bigQueryOptions = bigQueryOptions;
    this.tableRef = tableRef;
    this.bigQueryClient = null;
    this.type = type;
  }

  /** Builds a BigQueryReader directly using a BigQuery client. */
  public BigQueryReader(Bigquery bigQueryClient, TableReference tableRef, Class<T> type) {
    this.bigQueryOptions = null;
    this.tableRef = tableRef;
    this.bigQueryClient = bigQueryClient;
    this.type = type;
  }

  @Override
  public ReaderIterator<WindowedValue<T>> iterator() throws IOException {
    return new BigQueryReaderIterator(
        bigQueryClient != null
            ? bigQueryClient : Transport.newBigQueryClient(bigQueryOptions).build(),
        tableRef,
        type);
  }

  /**
   * A ReaderIterator that yields TableRow objects for each row of a BigQuery table.
   */
  class BigQueryReaderIterator extends AbstractReaderIterator<WindowedValue<T>> {
    private AbstractBigQueryIterator<T> rowIterator;

    public BigQueryReaderIterator(Bigquery bigQueryClient, TableReference tableRef, Class<T> type) {
      if (type == TableRow.class) {
        rowIterator = (AbstractBigQueryIterator<T>) new BigQueryTableRowIterator(
          bigQueryClient,
          tableRef);
      } else {
        rowIterator = new BigQueryTypedIterator(bigQueryClient, tableRef, type);
      }
    }

    @Override
    public boolean hasNext() {
      return rowIterator.hasNext();
    }

    @Override
    public WindowedValue<T> next() throws IOException {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return WindowedValue.valueInGlobalWindow(rowIterator.next());
    }

    @Override
    public Progress getProgress() {
      // For now reporting progress is not supported because this source is used only when
      // an entire table needs to be read by each worker (used as a side input for instance).
      throw new UnsupportedOperationException();
    }

    @Override
    public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest splitRequest) {
      // For now dynamic splitting is not supported because this source
      // is used only when an entire table needs to be read by each worker (used
      // as a side input for instance).
      throw new UnsupportedOperationException();
    }
  }
}

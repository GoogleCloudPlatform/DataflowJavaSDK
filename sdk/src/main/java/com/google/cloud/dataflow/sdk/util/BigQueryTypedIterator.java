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

import com.google.api.client.util.Preconditions;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.dataflow.sdk.util.bqmap.BqTypeMapper;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Iterates over all rows in a table.
 *
 * @param <T> the type of the elements read from the source
 */
public class BigQueryTypedIterator<T> extends AbstractBigQueryIterator<T> implements Closeable {

  private BqTypeMapper mapper;
  private Class<T> type;


  public BigQueryTypedIterator(Bigquery client, TableReference ref, Class<T> type) {
    super(client, ref);
    createFieldMapper(type);
  }

  private void createFieldMapper(Class<T> type) {
    this.type = type;
  }

  /**
   * Adjusts a field returned from the API to
   * match the type that will be seen when run on the
   * backend service. The end result is:
   *
   * <p><ul>
   *   <li> Nulls are {@code null}.
   *   <li> Repeated fields are lists.
   *   <li> Record columns are {@link TableRow}s.
   *   <li> {@code BOOLEAN} columns are JSON booleans, hence Java {@link Boolean}s.
   *   <li> {@code FLOAT} columns are JSON floats, hence Java {@link Double}s.
   *   <li> {@code TIMESTAMP} columns are {@link String}s that are of the format
   *        {yyyy-MM-dd HH:mm:ss.SSS UTC}.
   *   <li> Every other atomic type is a {@link String}.
   * </ul></p>
   *
   * <p> Note that currently integers are encoded as strings to match
   * the behavior of the backend service.
   */
  private void setField(TableFieldSchema fieldSchema, Object o, Object v)
    throws InstantiationException, IllegalAccessException {
    // In the input from the BQ API, atomic types all come in as
    // strings, while on the Dataflow service they have more precise
    // types.
    String name = fieldSchema.getName();
    mapper.set(name, o, v);
  }

  @Override
  protected T getTypedTableRow(List<TableFieldSchema> fields, Map<String, Object> rawRow) {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> cells = (List<Map<String, Object>>) rawRow.get("f");
    Preconditions.checkState(cells.size() == fields.size());

    Iterator<Map<String, Object>> cellIt = cells.iterator();
    Iterator<TableFieldSchema> fieldIt = fields.iterator();

    try {
      T row = type.newInstance();
      while (cellIt.hasNext()) {
        Map<String, Object> cell = cellIt.next();
        TableFieldSchema fieldSchema = fieldIt.next();
        setField(fieldSchema, row, cell.get("v"));
      }
      return row;
    } catch (InstantiationException e) {
      throw new RuntimeException(
        "Can't create type",
        e
      );
    } catch (IllegalAccessException e) {
      throw new RuntimeException(
        "One of the fields are not accessible",
        e
      );
    }
  }

  @Override
  protected void buildMapper() {
    this.mapper = new BqTypeMapper(type, schema);
  }
}

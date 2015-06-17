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

import com.google.api.client.util.Data;
import com.google.api.client.util.Preconditions;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Iterates over all rows in a table.
 */
public class BigQueryTableRowIterator extends AbstractBigQueryIterator<TableRow>
  implements Closeable {

  public BigQueryTableRowIterator(Bigquery client, TableReference ref) {
    super(client, ref);
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
  private Object getTypedCellValue(TableFieldSchema fieldSchema, Object v) {
    // In the input from the BQ API, atomic types all come in as
    // strings, while on the Dataflow service they have more precise
    // types.

    if (Data.isNull(v)) {
      return null;
    }

    if (Objects.equals(fieldSchema.getMode(), "REPEATED")) {
      TableFieldSchema elementSchema = fieldSchema.clone().setMode("REQUIRED");
      @SuppressWarnings("unchecked")
      List<Map<String, Object>> rawValues = (List<Map<String, Object>>) v;
      List<Object> values = new ArrayList<Object>(rawValues.size());
      for (Map<String, Object> element : rawValues) {
        values.add(getTypedCellValue(elementSchema, element.get("v")));
      }
      return values;
    }

    if (fieldSchema.getType().equals("RECORD")) {
      @SuppressWarnings("unchecked")
      Map<String, Object> typedV = (Map<String, Object>) v;
      return getTypedTableRow(fieldSchema.getFields(), typedV);
    }

    if (fieldSchema.getType().equals("FLOAT")) {
      return Double.parseDouble((String) v);
    }

    if (fieldSchema.getType().equals("BOOLEAN")) {
      return Boolean.parseBoolean((String) v);
    }

    if (fieldSchema.getType().equals("TIMESTAMP")) {
      // Seconds to milliseconds
      long milliSecs = (new Double(Double.parseDouble((String) v) * 1000)).longValue();
      DateTimeFormatter formatter =
          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC();
      return formatter.print(milliSecs) + " UTC";
    }

    return v;
  }

  protected TableRow getTypedTableRow(List<TableFieldSchema> fields, Map<String, Object> rawRow) {
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> cells = (List<Map<String, Object>>) rawRow.get("f");
    Preconditions.checkState(cells.size() == fields.size());

    Iterator<Map<String, Object>> cellIt = cells.iterator();
    Iterator<TableFieldSchema> fieldIt = fields.iterator();

    TableRow row = new TableRow();
    while (cellIt.hasNext()) {
      Map<String, Object> cell = cellIt.next();
      TableFieldSchema fieldSchema = fieldIt.next();
      row.set(fieldSchema.getName(), getTypedCellValue(fieldSchema, cell.get("v")));
    }
    return row;
  }

  @Override
  protected void buildMapper() {

  }
}

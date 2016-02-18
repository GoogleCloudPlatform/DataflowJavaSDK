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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllResponse;
import com.google.api.services.bigquery.model.TableDataList;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests for util classes related to BigQuery.
 */
@RunWith(JUnit4.class)
public class BigQueryUtilTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock private Bigquery mockClient;
  @Mock private Bigquery.Tables mockTables;
  @Mock private Bigquery.Tables.Get mockTablesGet;
  @Mock private Bigquery.Tabledata mockTabledata;
  @Mock private Bigquery.Tabledata.InsertAll mockInsertAll;
  @Mock private Bigquery.Tabledata.List mockTabledataList;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockClient);
    verifyNoMoreInteractions(mockTables);
    verifyNoMoreInteractions(mockTablesGet);
    verifyNoMoreInteractions(mockTabledata);
    verifyNoMoreInteractions(mockTabledataList);
  }

  private void onInsertAll(List<List<Long>> errorIndicesSequence) throws Exception {
    when(mockClient.tabledata())
        .thenReturn(mockTabledata);

    List<TableDataInsertAllResponse> responses = new ArrayList<>();
    for (List<Long> errorIndices : errorIndicesSequence) {
      List<TableDataInsertAllResponse.InsertErrors> errors = new ArrayList<>();
      for (long i : errorIndices) {
        TableDataInsertAllResponse.InsertErrors error =
            new TableDataInsertAllResponse.InsertErrors();
        error.setIndex(i);
      }
      TableDataInsertAllResponse response = new TableDataInsertAllResponse();
      response.setInsertErrors(errors);
      responses.add(response);
    }


    when(mockTabledata.insertAll(
        anyString(), anyString(), anyString(), any(TableDataInsertAllRequest.class)))
        .thenReturn(mockInsertAll);
    when(mockInsertAll.execute())
        .thenReturn(responses.get(0),
            responses.subList(1, responses.size()).toArray(
                new TableDataInsertAllResponse[responses.size() - 1]));
  }

  private void verifyInsertAll(int expectedRetries) throws IOException {
    verify(mockClient, times(expectedRetries)).tabledata();
    verify(mockTabledata, times(expectedRetries))
        .insertAll(anyString(), anyString(), anyString(), any(TableDataInsertAllRequest.class));
  }

  private void onTableGet(Table table) throws IOException {
    when(mockClient.tables())
        .thenReturn(mockTables);
    when(mockTables.get(anyString(), anyString(), anyString()))
        .thenReturn(mockTablesGet);
    when(mockTablesGet.execute())
        .thenReturn(table);
  }

  private void verifyTableGet() throws IOException {
    verify(mockClient).tables();
    verify(mockTables).get("project", "dataset", "table");
    verify(mockTablesGet, atLeastOnce()).execute();
  }

  private void onTableList(TableDataList result) throws IOException {
    when(mockClient.tabledata())
        .thenReturn(mockTabledata);
    when(mockTabledata.list(anyString(), anyString(), anyString()))
        .thenReturn(mockTabledataList);
    when(mockTabledataList.execute())
        .thenReturn(result);
  }

  private void verifyTabledataList() throws IOException {
    verify(mockClient, atLeastOnce()).tabledata();
    verify(mockTabledata, atLeastOnce()).list("project", "dataset", "table");
    verify(mockTabledataList, atLeastOnce()).execute();
    // Max results may be set when testing for an empty table.
    verify(mockTabledataList, atLeast(0)).setMaxResults(anyLong());
  }

  private Table basicTableSchema() {
    return new Table()
        .setSchema(new TableSchema()
            .setFields(Arrays.asList(
                new TableFieldSchema()
                    .setName("name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setName("answer")
                    .setType("INTEGER")
            )));
  }

  private Table basicTableSchemaWithTime() {
    return new Table()
        .setSchema(new TableSchema()
            .setFields(Arrays.asList(
                new TableFieldSchema()
                    .setName("name")
                    .setType("STRING"),
                new TableFieldSchema()
                    .setName("time")
                    .setType("TIMESTAMP"),
                new TableFieldSchema()
                    .setName("answer")
                    .setType("INTEGER")
            )));
  }

  @Test
  public void testReadWithTime() throws IOException {
    onTableGet(basicTableSchemaWithTime());

    TableDataList dataList = rawDataList(rawRow("Arthur", "1.430397296789E9", 42));
    onTableList(dataList);

    try (BigQueryTableRowIterator iterator = BigQueryTableRowIterator.fromTable(
        BigQueryIO.parseTableSpec("project:dataset.table"),
        mockClient)) {

      Assert.assertTrue(iterator.hasNext());
      TableRow row = iterator.next();

      Assert.assertTrue(row.containsKey("name"));
      Assert.assertTrue(row.containsKey("time"));
      Assert.assertTrue(row.containsKey("answer"));
      Assert.assertEquals("Arthur", row.get("name"));
      Assert.assertEquals("2015-04-30 12:34:56.789 UTC", row.get("time"));
      Assert.assertEquals(42, row.get("answer"));

      Assert.assertFalse(iterator.hasNext());

      verifyTableGet();
      verifyTabledataList();
    }
  }

  private TableRow rawRow(Object...args) {
    List<TableCell> cells = new LinkedList<>();
    for (Object a : args) {
      cells.add(new TableCell().setV(a));
    }
    return new TableRow().setF(cells);
  }

  private TableDataList rawDataList(TableRow...rows) {
    return new TableDataList()
        .setRows(Arrays.asList(rows));
  }

  @Test
  public void testRead() throws IOException {
    onTableGet(basicTableSchema());

    TableDataList dataList = rawDataList(rawRow("Arthur", 42));
    onTableList(dataList);

    try (BigQueryTableRowIterator iterator = BigQueryTableRowIterator.fromTable(
        BigQueryIO.parseTableSpec("project:dataset.table"),
        mockClient)) {

      Assert.assertTrue(iterator.hasNext());
      TableRow row = iterator.next();

      Assert.assertTrue(row.containsKey("name"));
      Assert.assertTrue(row.containsKey("answer"));
      Assert.assertEquals("Arthur", row.get("name"));
      Assert.assertEquals(42, row.get("answer"));

      Assert.assertFalse(iterator.hasNext());

      verifyTableGet();
      verifyTabledataList();
    }
  }

  @Test
  public void testReadEmpty() throws IOException {
    onTableGet(basicTableSchema());

    // BigQuery may respond with a page token for an empty table, ensure we
    // handle it.
    TableDataList dataList = new TableDataList()
        .setPageToken("FEED==")
        .setTotalRows(0L);
    onTableList(dataList);

    try (BigQueryTableRowIterator iterator = BigQueryTableRowIterator.fromTable(
        BigQueryIO.parseTableSpec("project:dataset.table"),
        mockClient)) {

      Assert.assertFalse(iterator.hasNext());

      verifyTableGet();
      verifyTabledataList();
    }
  }

  @Test
  public void testReadMultiPage() throws IOException {
    onTableGet(basicTableSchema());

    TableDataList page1 = rawDataList(rawRow("Row1", 1))
        .setPageToken("page2");
    TableDataList page2 = rawDataList(rawRow("Row2", 2))
        .setTotalRows(2L);

    when(mockClient.tabledata())
        .thenReturn(mockTabledata);
    when(mockTabledata.list(anyString(), anyString(), anyString()))
        .thenReturn(mockTabledataList);
    when(mockTabledataList.execute())
        .thenReturn(page1)
        .thenReturn(page2);

    try (BigQueryTableRowIterator iterator = BigQueryTableRowIterator.fromTable(
        BigQueryIO.parseTableSpec("project:dataset.table"),
        mockClient)) {

      List<String> names = new LinkedList<>();
      Iterators.addAll(names,
          Iterators.transform(iterator, new Function<TableRow, String>(){
            @Override
            public String apply(TableRow input) {
              return (String) input.get("name");
            }
          }));

      Assert.assertThat(names, Matchers.hasItems("Row1", "Row2"));

      verifyTableGet();
      verifyTabledataList();
      // The second call should have used a page token.
      verify(mockTabledataList).setPageToken("page2");
    }
  }

  @Test
  public void testReadOpenFailure() throws IOException {
    thrown.expect(RuntimeException.class);

    when(mockClient.tables())
        .thenReturn(mockTables);
    when(mockTables.get(anyString(), anyString(), anyString()))
        .thenReturn(mockTablesGet);
    when(mockTablesGet.execute())
        .thenThrow(new IOException("No such table"));

    try (BigQueryTableRowIterator iterator = BigQueryTableRowIterator.fromTable(
        BigQueryIO.parseTableSpec("project:dataset.table"),
        mockClient)) {
      try {
        Assert.assertFalse(iterator.hasNext());  // throws.
      } finally {
        verifyTableGet();
      }
    }
  }

  @Test
  public void testWriteAppend() throws IOException {
    onTableGet(basicTableSchema());

    TableReference ref = BigQueryIO
        .parseTableSpec("project:dataset.table");

    BigQueryTableInserter inserter = new BigQueryTableInserter(mockClient);

    inserter.getOrCreateTable(ref, BigQueryIO.Write.WriteDisposition.WRITE_APPEND,
        BigQueryIO.Write.CreateDisposition.CREATE_NEVER, null);

    verifyTableGet();
  }

  @Test
  public void testWriteEmpty() throws IOException {
    onTableGet(basicTableSchema());

    TableDataList dataList = new TableDataList().setTotalRows(0L);
    onTableList(dataList);

    TableReference ref = BigQueryIO
        .parseTableSpec("project:dataset.table");

    BigQueryTableInserter inserter = new BigQueryTableInserter(mockClient);

    inserter.getOrCreateTable(ref, BigQueryIO.Write.WriteDisposition.WRITE_EMPTY,
        BigQueryIO.Write.CreateDisposition.CREATE_NEVER, null);

    verifyTableGet();
    verifyTabledataList();
  }

  @Test
  public void testWriteEmptyFail() throws IOException {
    thrown.expect(IOException.class);

    onTableGet(basicTableSchema());

    TableDataList dataList = rawDataList(rawRow("Arthur", 42));
    onTableList(dataList);

    TableReference ref = BigQueryIO
        .parseTableSpec("project:dataset.table");

    BigQueryTableInserter inserter = new BigQueryTableInserter(mockClient);

    try {
      inserter.getOrCreateTable(ref, BigQueryIO.Write.WriteDisposition.WRITE_EMPTY,
          BigQueryIO.Write.CreateDisposition.CREATE_NEVER, null);
    } finally {
      verifyTableGet();
      verifyTabledataList();
    }
  }

  @Test
  public void testInsertAll() throws Exception, IOException {
    // Build up a list of indices to fail on each invocation. This should result in
    // 5 calls to insertAll.
    List<List<Long>> errorsIndices = new ArrayList<>();
    errorsIndices.add(Arrays.asList(0L, 5L, 10L, 15L, 20L));
    errorsIndices.add(Arrays.asList(0L, 2L, 4L));
    errorsIndices.add(Arrays.asList(0L, 2L));
    errorsIndices.add(new ArrayList<Long>());
    onInsertAll(errorsIndices);

    TableReference ref = BigQueryIO
        .parseTableSpec("project:dataset.table");
    BigQueryTableInserter inserter = new BigQueryTableInserter(mockClient, 5);

    List<TableRow> rows = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 25; ++i) {
      rows.add(new TableRow());
      ids.add(new String());
    }

    try {
      inserter.insertAll(ref, rows, ids);
    } finally {
      verifyInsertAll(5);
    }
  }
}

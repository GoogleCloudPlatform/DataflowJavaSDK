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

package com.google.cloud.dataflow.sdk.util.bqmap;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BqTypeMapper {

  private Map<String, BqField> map = new HashMap<>();


  //    if (Data.isNull(v)) {
//      return null;
//    }
//
//    if (Objects.equals(fieldSchema.getMode(), "REPEATED")) {
//      TableFieldSchema elementSchema = fieldSchema.clone().setMode("REQUIRED");
//      @SuppressWarnings("unchecked")
//      List<Map<String, Object>> rawValues = (List<Map<String, Object>>) v;
//      List<Object> values = new ArrayList<Object>(rawValues.size());
//      for (Map<String, Object> element : rawValues) {
//        values.add(getTypedCellValue(elementSchema, element.get("v")));
//      }
//      return values;
//    }
//
//    if (fieldSchema.getType().equals("RECORD")) {
//      @SuppressWarnings("unchecked")
//      Map<String, Object> typedV = (Map<String, Object>) v;
//      return getTypedTableRow(fieldSchema.getFields(), typedV);
//    }
//
//    if (fieldSchema.getType().equals("FLOAT")) {
//      return Double.parseDouble((String) v);
//    }
//
//    if (fieldSchema.getType().equals("BOOLEAN")) {
//      return Boolean.parseBoolean((String) v);
//    }
//
//    if (fieldSchema.getType().equals("TIMESTAMP")) {
//      // Seconds to milliseconds
//      long milliSecs = (new Double(Double.parseDouble((String) v) * 1000)).longValue();
//      DateTimeFormatter formatter =
//          DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC();
//      return formatter.print(milliSecs) + " UTC";
//    }
//
//    return v;
  public <T> BqTypeMapper(Class<T> type, TableSchema schema) {
    Field[] typeFields = type.getFields();


    List<TableFieldSchema> fields = schema.getFields();
    for (TableFieldSchema field : fields) {
      String name = field.getName();
      // First pass looking for annotations
      for (Field typeField : typeFields) {
        BqColumn annotation = typeField.getAnnotation(BqColumn.class);
        if (annotation != null) {
          if (name.equals(annotation.name())) {
            Class<?> t = typeField.getType();

            System.out.println(t.toString());
            map.put(name, new BqFieldDebug(typeField));
            continue;
            //map.put(name,)
          }
        }
      }
      // Second pass looking field names
      try {
        Field typeField = type.getField(name);
        map.put(name, new BqFieldDebug(typeField));
        continue;
      } catch (NoSuchFieldException e) {
      }
      // default
      if (map.get(name) == null) {
        map.put(name, new BqFieldNoop());
      }
    }
  }

  public void set(String name, Object o, Object v) {
    BqField bqFieldMap = map.get(name);
    bqFieldMap.set(o, v);
  }


}

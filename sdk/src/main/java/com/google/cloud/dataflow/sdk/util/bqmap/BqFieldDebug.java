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

import java.lang.reflect.Field;

/**
 *
 */
class BqFieldDebug extends BqField {

  private Field field;

  public BqFieldDebug(Field field) {
    this.field = field;
  }

  public void set(Object o, Object v) {
    String value = (String) v;
    Class<?> type = field.getType();
    if (type == Integer.class) {
      try {
        field.set(o, Integer.valueOf(value));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    } else if (type == String.class) {
      try {
        field.set(o, value);
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    } else if (type == Double.class) {
      try {
        field.set(o, Double.valueOf(value));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    } else if (type == Float.class) {
      try {
        field.set(o, Float.valueOf(value));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }

    System.out.println();

  }
}

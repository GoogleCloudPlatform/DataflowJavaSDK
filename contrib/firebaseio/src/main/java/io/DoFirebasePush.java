/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not  use this file except  in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

import com.firebase.client.Firebase;

import java.util.AbstractMap;
import java.util.Map.Entry;

import utils.FirebaseAuthenticator;

/**
 * Creates a unique String ID using {@link Firebase#push()} and outputs an
 * {@link java.util.Map.Entry} with the String as a key.
 */
public class DoFirebasePush extends FirebaseDoFn<Object, Entry<String, Object>> {

  private static final long serialVersionUID = -2377431649046447957L;

  public DoFirebasePush(String url, FirebaseAuthenticator auther) {
    super(url, auther);
  }

  @Override
  public void processElement(DoFn<Object, Entry<String, Object>>.ProcessContext context) {
    context.output(new AbstractMap.SimpleImmutableEntry<String, Object>(
        root.push().getKey(), context.element()));
  }


  @Override
  public void asyncProcessElement(DoFn<Object, Entry<String, Object>>.ProcessContext context,
      FirebaseDoFn<Object, Entry<String, Object>>.FirebaseListener listener) {
    //Call is synchronous, this will not be called
    return;
  }

}



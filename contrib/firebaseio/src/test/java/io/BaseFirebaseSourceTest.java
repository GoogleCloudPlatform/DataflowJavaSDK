/**
 * Copyright (c) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebase.client.ChildEventListener;
import com.firebase.client.Firebase;
import com.firebase.client.Firebase.CompletionListener;
import com.firebase.client.FirebaseError;
import com.firebase.client.ValueEventListener;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import contrib.LogElements;

import events.FirebaseEvent;
import utils.FirebaseAuthenticator;
import utils.FirebaseEmptyAuthenticator;
import utils.LoggingArrayList;



/**
 * Archetype for comparing events produced by through {@link FirebaseSource} with events surfaced
 * by the unmodified {@link ChildEventListener} and {@link ValueEventListener}.
 **/
@RunWith(JUnit4.class)
public abstract class BaseFirebaseSourceTest {

  protected FirebaseAuthenticator auther = new FirebaseEmptyAuthenticator();
  private Throwable err;
  protected Firebase testRef;

  static final Logger LOGGER = LoggerFactory.getLogger(BaseFirebaseSourceTest.class);

  final LoggingArrayList<FirebaseEvent<JsonNode>> expected =
      new LoggingArrayList<FirebaseEvent<JsonNode>>(LOGGER);

  @Before
  public void setUp() throws JsonParseException,
  JsonMappingException, IOException, InterruptedException {

    testRef = new Firebase("https://dataflowio.firebaseio-demo.com")
        .child(Integer.toHexString(this.hashCode())
            + Long.toHexString(System.currentTimeMillis()));

    cleanFirebase(testRef);

    prepareData(new ObjectMapper().<List<Map<String, Object>>>readValue(
        ClassLoader.getSystemResourceAsStream("testdata.json"),
        new TypeReference<List<Object>>() {}));

  }

  @After
  public void teardown() throws InterruptedException{
    cleanFirebase(testRef);
  }

  protected Thread getPipelineThread(final Pipeline p){
    Thread t = new Thread(new Runnable(){
      @Override
      public void run() {
        LOGGER.info(p.run().getState().toString());
      }
    });
    t.setUncaughtExceptionHandler(new UncaughtExceptionHandler(){
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        err = e;
      }
    });
    return t;
  }

  protected void cleanFirebase(Firebase f) throws InterruptedException{
    final Semaphore lock = new Semaphore(1);
    lock.acquire();
    f.removeValue(new CompletionListener(){
      @Override
      public void onComplete(FirebaseError arg0, Firebase arg1) {
        lock.release();
      }
    });
    lock.tryAcquire(Long.MAX_VALUE, TimeUnit.DAYS);

  }

  public abstract void prepareData(List<Map<String, Object>> testData);

  public abstract void triggerEvents(Firebase f);

  public abstract void addListener(Firebase f, final Collection<FirebaseEvent<JsonNode>> events);

  public abstract void removeListener(Firebase f);

  public abstract FirebaseSource<JsonNode> makeSource(FirebaseAuthenticator auther,
      Firebase f);

  @Test
  public void matchEvents() throws Throwable{

    //Generate expected events
    addListener(testRef, expected);
    triggerEvents(testRef);
    removeListener(testRef);
    LOGGER.info("At most " + expected.size() + " elements to look for in pipeline");

    cleanFirebase(testRef);

    FirebaseSource<JsonNode> source = makeSource(auther, testRef);

    TestPipeline p = TestPipeline.create();

    PCollection<FirebaseEvent<JsonNode>> events = p
        .apply(Read.from(source).withMaxNumRecords(expected.size()));

    events.apply(new LogElements<FirebaseEvent<JsonNode>>(FirebaseChildTest.class, "INFO"));

    DataflowAssert.that(events).containsInAnyOrder(expected);

    //Test pipeline against expected events;
    Thread t = getPipelineThread(p);
    t.start();
    Thread.sleep(1500);
    triggerEvents(testRef);
    t.join();
    if (err != null){
      throw err;
    }
  }

}

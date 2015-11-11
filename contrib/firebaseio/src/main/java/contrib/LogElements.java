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
package contrib;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs a {@link PCollection} to a logger for the specified {@link Class}.
 */
public class LogElements<T> extends PTransform<PCollection<T>, PDone> {

  private static class LoggingDoFn<T> extends DoFn<T, Void>{

    private final String className;
    private final String logLevel;
    private transient Logger logger;

    public LoggingDoFn(String className, String logLevel){
      this.logLevel = logLevel;
      this.className = className;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
      logger = LoggerFactory.getLogger(Class.forName(className));
    }

    @Override
    public void processElement(DoFn<T, Void>.ProcessContext c) throws Exception {
      logger.info(c.element().toString());
    }
  }

  private final String className;
  private final String logLevel;

  public LogElements(Class<?> clazz, String logLevel){
    this.logLevel = logLevel;
    this.className = clazz.getName();
  }

  @Override
  public PDone apply(PCollection<T> input){
    input.apply(ParDo.of(new LoggingDoFn<T>(className, logLevel)));
    return PDone.in(input.getPipeline());
  }
}

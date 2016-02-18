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

import static com.google.cloud.dataflow.sdk.runners.worker.SourceOperationExecutor.SPLIT_RESPONSE_TOO_LARGE_ERROR;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceOperationExecutor.isSplitResponseTooLarge;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toCloudPosition;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.api.services.dataflow.model.SourceOperationResponse;
import com.google.api.services.dataflow.model.Status;
import com.google.api.services.dataflow.model.WorkItem;
import com.google.api.services.dataflow.model.WorkItemServiceState;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.dataflow.CustomSources;
import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingHandler;
import com.google.cloud.dataflow.sdk.util.BatchModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.CloudCounterUtils;
import com.google.cloud.dataflow.sdk.util.CloudMetricUtils;
import com.google.cloud.dataflow.sdk.util.PCollectionViewWindow;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.Sized;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.Metric;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.common.worker.WorkExecutor;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This is a semi-abstract harness for executing WorkItem tasks in
 * Java workers. Concrete implementations need to implement a
 * WorkUnitClient.
 *
 * <p>DataflowWorker presents one public interface,
 * getAndPerformWork(), which uses the WorkUnitClient to get work,
 * execute it, and update the work.
 */
public class DataflowWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowWorker.class);

  /**
   * A client to get and update work items.
   */
  private final WorkUnitClient workUnitClient;

  /**
   * Pipeline options, initially provided via the constructor and
   * partially provided via each work work unit.
   */
  private final DataflowWorkerHarnessOptions options;

  /**
   * A side input cache shared between all execution contexts.
   */
  private final Cache<PCollectionViewWindow<?>, Sized<Object>> sideInputCache;

  /**
   * Status server returning health of worker.
   */
  private Server statusServer;

  /**
   * Tracker for user code time.
   */
  private final UserCodeTimeTracker userCodeTimeTracker = new UserCodeTimeTracker();

  /**
   * A weight in "bytes" for the overhead of a {@link Sized} wrapper in the cache. It is just an
   * approximation so it is OK for it to be fairly arbitrary as long as it is nonzero.
   */
  private static final int OVERHEAD_WEIGHT = 8;

  private static final int MEGABYTES = 1024 * 1024;

  public static final int DEFAULT_STATUS_PORT = 18081;

  public DataflowWorker(WorkUnitClient workUnitClient, DataflowWorkerHarnessOptions options) {
    this.workUnitClient = workUnitClient;
    this.options = options;
    this.sideInputCache = CacheBuilder.newBuilder()
        .maximumWeight(options.getWorkerCacheMb() * MEGABYTES) // weights are in bytes
        .weigher(SizedWeigher.<PCollectionViewWindow<?>, Object>withBaseWeight(OVERHEAD_WEIGHT))
        .softValues()
        .build();
  }

  /**
   * Gets WorkItem and performs it; returns true if work was
   * successfully completed.
   *
   * <p>getAndPerformWork may throw if there is a failure of the
   * WorkUnitClient.
   */
  public boolean getAndPerformWork() throws IOException {
    WorkItem work = workUnitClient.getWorkItem();
    if (work == null) {
      return false;
    }
    return doWork(work);
  }

  /**
   * Performs the given work; returns true if successful.
   *
   * @throws IOException Only if the WorkUnitClient fails.
   */
  private boolean doWork(WorkItem workItem) throws IOException {
    LOG.debug("Executing: {}", workItem);

    WorkExecutor worker = null;
    SourceOperationResponse operationResponse = null;
    long nextReportIndex = workItem.getInitialReportIndex();
    try {
      // Populate PipelineOptions with data from work unit.
      options.setProject(workItem.getProjectId());

      DataflowExecutionContext executionContext =
          new DataflowWorkerExecutionContext(sideInputCache, options);

      CounterSet counters = new CounterSet();
      StateSampler sampler = null;

      if (workItem.getMapTask() != null) {
        sampler = new StateSampler(
            workItem.getMapTask().getStageName() + "-", counters.getAddCounterMutator());
        worker = MapTaskExecutorFactory.create(
            options, workItem.getMapTask(), executionContext, counters, sampler);
      } else if (workItem.getSourceOperationTask() != null) {
        sampler = new StateSampler(
            "source-operation-", counters.getAddCounterMutator());
        worker = SourceOperationExecutorFactory.create(options, workItem.getSourceOperationTask());
      } else {
        throw new RuntimeException("Unknown kind of work item: " + workItem.toString());
      }

      sampler.addSamplingCallback(
          new UserCodeTimeTracker.StateSamplerCallback(
              userCodeTimeTracker, workItem.getId()));

      DataflowWorkProgressUpdater progressUpdater =
          new DataflowWorkProgressUpdater(workItem, worker, workUnitClient, options);
      try (AutoCloseable scope = userCodeTimeTracker.scopedWork(
              sampler.getPrefix(), workItem.getId(), counters.getAddCounterMutator())) {
        // Nested try/finally is used to make sure worker.close() happen before scope.close().
        try {
          executeWork(worker, progressUpdater);
        } finally {
          worker.close();
          // Grab nextReportIndex so we can use it in handleWorkError if there is an exception.
          nextReportIndex = progressUpdater.getNextReportIndex();
        }
      }

      // Log all counter values for debugging purposes.
      for (Counter<?> counter : counters) {
        LOG.trace("COUNTER {}.", counter);
      }

      // Log all metrics for debugging purposes.
      Collection<Metric<?>> metrics = worker.getOutputMetrics();
      for (Metric<?> metric : metrics) {
        LOG.trace("METRIC {}: {}", metric.getName(), metric.getValue());
      }

      // Report job success.
      // TODO: Find out a generic way for the WorkExecutor to report work-specific results
      // into the work update.
      operationResponse =
          (worker instanceof SourceOperationExecutor)
              ? ((SourceOperationExecutor) worker).getResponse()
              : null;

      try {
        reportStatus(
          options, "Success", workItem, counters, metrics, operationResponse, null/*errors*/,
          nextReportIndex);
      } catch (GoogleJsonResponseException e) {
        if ((operationResponse != null) && (worker instanceof SourceOperationExecutor)) {
          if (isSplitResponseTooLarge(operationResponse)) {
            throw new RuntimeException(SPLIT_RESPONSE_TOO_LARGE_ERROR, e);
          }
        }
        throw e;
      }

      return true;

    } catch (Throwable e) {
      handleWorkError(workItem, worker, nextReportIndex, e);
      return false;

    } finally {
      if (worker != null) {
        try {
          worker.close();
        } catch (Exception exn) {
          LOG.warn("Uncaught exception occurred during work unit shutdown:", exn);
        }
      }
    }
  }

  /** Executes the work and report progress. For testing only. */
  void executeWork(WorkExecutor worker, DataflowWorkProgressUpdater progressUpdater)
      throws Exception {
    progressUpdater.startReportingProgress();
    // Blocks while executing the work.
    try {
      worker.execute();
    } finally {
      // stopReportingProgress can throw an exception if the final progress
      // update fails. For correctness, the task must then be marked as failed.
      progressUpdater.stopReportingProgress();
    }
  }


  /** Handles the exception thrown when reading and executing the work. */
  private void handleWorkError(WorkItem workItem, WorkExecutor worker, long nextReportIndex,
      Throwable e) throws IOException {
    LOG.warn("Uncaught exception occurred during work unit execution:", e);

    // TODO: Look into moving the stack trace thinning
    // into the client.
    Throwable t = e instanceof UserCodeException ? e.getCause() : e;
    Status error = new Status();
    error.setCode(2); // Code.UNKNOWN.  TODO: Replace with a generated definition.
    // TODO: Attach the stack trace as exception details, not to the message.
    error.setMessage(DataflowWorkerLoggingHandler.formatException(t));

    reportStatus(options, "Failure", workItem, worker == null ? null : worker.getOutputCounters(),
        worker == null ? null : worker.getOutputMetrics(), null/*sourceOperationResponse*/,
        error == null ? null : Collections.singletonList(error), nextReportIndex);
  }

  private void reportStatus(DataflowWorkerHarnessOptions options, String status, WorkItem workItem,
      @Nullable CounterSet counters, @Nullable Collection<Metric<?>> metrics,
      @Nullable SourceOperationResponse operationResponse, @Nullable List<Status> errors,
      long reportIndex)
      throws IOException {
    String message = "{} processing work item {}";
    if (null != errors && errors.size() > 0) {
      LOG.warn(message, status, uniqueId(workItem));
    } else {
      LOG.debug(message, status, uniqueId(workItem));
    }
    WorkItemStatus workItemStatus = buildStatus(workItem, true/*completed*/, counters, metrics,
        options, null, null, operationResponse, errors, reportIndex);
    workUnitClient.reportWorkItemStatus(workItemStatus);
  }

  static WorkItemStatus buildStatus(WorkItem workItem, boolean completed,
      @Nullable CounterSet counters, @Nullable Collection<Metric<?>> metrics,
      DataflowWorkerHarnessOptions options, @Nullable Reader.Progress progress,
      @Nullable Reader.DynamicSplitResult dynamicSplitResult,
      @Nullable SourceOperationResponse operationResponse, @Nullable List<Status> errors,
      long reportIndex) {

    return buildStatus(workItem, completed, counters, metrics, options, progress,
        dynamicSplitResult, operationResponse, errors, reportIndex, null);
  }

  static WorkItemStatus buildStatus(WorkItem workItem, boolean completed,
      @Nullable CounterSet counters, @Nullable Collection<Metric<?>> metrics,
      DataflowWorkerHarnessOptions options, @Nullable Reader.Progress progress,
      @Nullable Reader.DynamicSplitResult dynamicSplitResult,
      @Nullable SourceOperationResponse operationResponse, @Nullable List<Status> errors,
      long reportIndex,
      @Nullable StateSampler.StateSamplerInfo stateSamplerInfo) {
    WorkItemStatus status = new WorkItemStatus();
    status.setWorkItemId(Long.toString(workItem.getId()));
    status.setCompleted(completed);
    status.setReportIndex(reportIndex);

    List<MetricUpdate> counterUpdates = null;
    List<MetricUpdate> metricUpdates = null;

    if (counters != null) {
      // Currently we lack a reliable exactly-once delivery mechanism for
      // work updates, i.e. they can be retried or reordered, so sending
      // delta updates could lead to double-counted or missed contributions.
      // However, delta updates may be beneficial for performance.
      // TODO: Implement exactly-once delivery and use deltas,
      // if it ever becomes clear that deltas are necessary for performance.
      boolean delta = false;
      counterUpdates = CloudCounterUtils.extractCounters(counters, delta);
    }
    if (metrics != null) {
      metricUpdates = CloudMetricUtils.extractCloudMetrics(metrics, options.getWorkerId());
    }
    List<MetricUpdate> updates = new ArrayList<>();
    if (counterUpdates != null) {
      updates.addAll(counterUpdates);
    }
    if (metricUpdates != null) {
      updates.addAll(metricUpdates);
    }
    if (stateSamplerInfo != null) {
      MetricUpdate update = new MetricUpdate();
      update.setKind("internal");
      MetricStructuredName name = new MetricStructuredName();
      name.setName("state-sampler");
      update.setName(name);
      Map<String, Object> metric = new HashMap<String, Object>();
      if (stateSamplerInfo.state != null) {
        metric.put("last-state-name", stateSamplerInfo.state);
      }
      if (stateSamplerInfo.transitionCount != null) {
        metric.put("num-transitions", stateSamplerInfo.transitionCount);
      }
      if (stateSamplerInfo.stateDurationMillis != null) {
        metric.put("last-state-duration-ms",
            stateSamplerInfo.stateDurationMillis);
      }
      update.setInternal(metric);
      updates.add(update);
    }
    status.setMetricUpdates(updates);

    // TODO: Provide more structure representation of error,
    // e.g., the serialized exception object.
    if (errors != null) {
      status.setErrors(errors);
    }

    if (progress != null) {
      status.setProgress(readerProgressToCloudProgress(progress));
    }
    if (dynamicSplitResult instanceof Reader.DynamicSplitResultWithPosition) {
      Reader.DynamicSplitResultWithPosition asPosition =
          (Reader.DynamicSplitResultWithPosition) dynamicSplitResult;
      status.setStopPosition(toCloudPosition(asPosition.getAcceptedPosition()));
    } else if (dynamicSplitResult instanceof CustomSources.BoundedSourceSplit) {
      status.setDynamicSourceSplit(
          CustomSources.toSourceSplit(
              (CustomSources.BoundedSourceSplit<?>) dynamicSplitResult, options));
    } else if (dynamicSplitResult != null) {
      throw new IllegalArgumentException(
          "Unexpected type of dynamic split result: " + dynamicSplitResult);
    }

    if (workItem.getSourceOperationTask() != null) {
      status.setSourceOperationResponse(operationResponse);
    }

    return status;
  }

  static String uniqueId(WorkItem work) {
    return work.getProjectId() + ";" + work.getJobId() + ";" + work.getId();
  }

  /**
   * Abstract base class describing a client for WorkItem work units.
   */
  public abstract static class WorkUnitClient {
    /**
     * Returns a new WorkItem unit for this Worker to work on or null
     * if no work item is available.
     */
    public abstract WorkItem getWorkItem() throws IOException;

    /**
     * Reports a {@link WorkItemStatus} for an assigned {@link WorkItem}.
     *
     * @param workItemStatus the status to report
     * @return a {@link WorkItemServiceState} (e.g. a new stop position)
     */
    public abstract WorkItemServiceState reportWorkItemStatus(WorkItemStatus workItemStatus)
        throws IOException;
  }

  /**
   * A {@link DataflowExecutionContext} that provides a caching side input reader using
   * the worker's shared cache.
   */
  private static class DataflowWorkerExecutionContext extends BatchModeExecutionContext {

    private final Cache<PCollectionViewWindow<?>, Sized<Object>> cache;
    private final PipelineOptions options;

    public DataflowWorkerExecutionContext(
        Cache<PCollectionViewWindow<?>, Sized<Object>> cache, PipelineOptions options) {
      super(options);
      this.cache = cache;
      this.options = options;
    }

    @Override
    public SideInputReader getSideInputReader(Iterable<? extends SideInputInfo> sideInputInfos)
      throws Exception {
      return CachingSideInputReader.of(
          DataflowSideInputReader.of(sideInputInfos, options, this),
          cache);
    }

    @Override
    public SideInputReader getSideInputReaderForViews(
        Iterable<? extends PCollectionView<?>> sideInputViews) {
      throw new UnsupportedOperationException(
        "Cannot call getSideInputReaderForViews for batch DataflowWorker: "
        + "the MapTask specification should have had SideInputInfo descriptors "
        + "for each side input, and a SideInputReader provided via getSideInputReader");
    }
  }

  /**
   * Runs the status server to report worker health on the specified port.
   */
  public void runStatusServer(int statusPort) {
    LOG.info("Status server started on port {}", statusPort);
    runStatusServer(new Server(statusPort));
  }

  // @VisibleForTesting
  void runStatusServer(Server server) {
    statusServer = server;
    statusServer.setHandler(new StatusHandler());
    try {
      // Run status server in separate thread.
      statusServer.start();
    } catch (Exception e) {
      LOG.warn("Status server failed to start: ", e);
    }
  }

  private class StatusHandler extends AbstractHandler {
    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
        HttpServletResponse response) throws IOException, ServletException {
      response.setContentType("text/html;charset=utf-8");
      baseRequest.setHandled(true);

      PrintWriter responseWriter = response.getWriter();

      if (target.equals("/healthz")) {
        response.setStatus(HttpServletResponse.SC_OK);
        // Right now, we always return "ok".
        responseWriter.println("ok");
      } else if (target.equals("/threadz")) {
        response.setStatus(HttpServletResponse.SC_OK);
        printThreads(responseWriter);
      } else {
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        responseWriter.println("not found");
      }
    }

    private void printThreads(PrintWriter response) {
      Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
      for (Map.Entry<Thread,  StackTraceElement[]> entry : stacks.entrySet()) {
        Thread thread = entry.getKey();
        response.println("--- Thread: " + thread + " State: "
            + thread.getState() + " stack: ---");
        for (StackTraceElement element : entry.getValue()) {
          response.println("  " + element);
        }
      }
    }
  }
}

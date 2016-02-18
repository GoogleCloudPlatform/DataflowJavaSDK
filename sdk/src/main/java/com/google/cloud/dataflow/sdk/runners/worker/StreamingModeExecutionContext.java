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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.runners.dataflow.CustomSources;
import com.google.cloud.dataflow.sdk.runners.worker.StateFetcher.SideInputState;
import com.google.cloud.dataflow.sdk.runners.worker.StreamingDataflowWorker.ReaderCacheEntry;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.BaseExecutionContext;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.TimeDomain;
import com.google.cloud.dataflow.sdk.util.TimerInternals;
import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler.StateKind;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * {@link ExecutionContext} for use in streaming mode.
 */
public class StreamingModeExecutionContext extends DataflowExecutionContext {
  private final String stageName;
  private final Map<TupleTag<?>, Map<BoundedWindow, Object>> sideInputCache;
  // Per-key cache of active Reader objects in use by this process.
  private final ConcurrentMap<ByteString, ReaderCacheEntry> readerCache;
  private final ConcurrentMap<String, String> stateNameMap;

  private Windmill.WorkItem work;
  private Instant inputDataWatermark;
  private WindmillStateReader stateReader;
  private StateFetcher stateFetcher;
  private Windmill.WorkItemCommitRequest.Builder outputBuilder;
  private UnboundedSource.UnboundedReader<?> activeReader;

  public StreamingModeExecutionContext(
      String stageName,
      ConcurrentMap<ByteString, ReaderCacheEntry> readerCache,
      ConcurrentMap<String, String> stateNameMap) {
    this.stageName = stageName;
    this.sideInputCache = new HashMap<>();
    this.readerCache = readerCache;
    this.stateNameMap = stateNameMap;
  }

  public void start(
      Windmill.WorkItem work,
      Instant inputDataWatermark,
      WindmillStateReader stateReader,
      StateFetcher stateFetcher,
      Windmill.WorkItemCommitRequest.Builder outputBuilder) {
    this.work = work;
    this.inputDataWatermark = inputDataWatermark;
    this.stateReader = stateReader;
    this.stateFetcher = stateFetcher;
    this.outputBuilder = outputBuilder;
    this.sideInputCache.clear();

    for (ExecutionContext.StepContext stepContext : getAllStepContexts()) {
      ((StepContext) stepContext).start(stateReader, inputDataWatermark);
    }
  }

  @Override
  public ExecutionContext.StepContext createStepContext(
      String stepName, String transformName, StateSampler stateSampler) {
    StepContext context = new StepContext(stepName, transformName, stateSampler);
    context.start(stateReader, inputDataWatermark);
    return context;
  }

  @Override
  public SideInputReader getSideInputReader(Iterable<? extends SideInputInfo> sideInputInfos) {
    throw new UnsupportedOperationException(
        "Cannot call getSideInputReader for StreamingDataflowWorker: "
        + "the MapTask specification should not have had any SideInputInfo descriptors "
        + "since the streaming runner does not yet support them.");
  }

  @Override
  public SideInputReader getSideInputReaderForViews(Iterable<? extends PCollectionView<?>> views) {
    return StreamingModeSideInputReader.of(views, this);
  }

  /**
   * Fetches the requested sideInput, and maintains a view of the cache that doesn't remove
   * items until the active work item is finished.
   */
  private <T> T fetchSideInput(PCollectionView<T> view, BoundedWindow sideInputWindow,
      String stateFamily, SideInputState state,
      Supplier<StateSampler.ScopedState> scopedReadStateSupplier) {
    Map<BoundedWindow, Object> tagCache = sideInputCache.get(view.getTagInternal());
    if (tagCache == null) {
      tagCache = new HashMap<>();
      sideInputCache.put(view.getTagInternal(), tagCache);
    }

    @SuppressWarnings("unchecked")
    T sideInput = (T) tagCache.get(sideInputWindow);
    if (sideInput == null) {
      if (state == SideInputState.CACHED_IN_WORKITEM) {
        throw new IllegalStateException(
            "Expected side input to be cached. Tag: "
            + view.getTagInternal().getId());
      }
      T typed = stateFetcher.fetchSideInput(
          view, sideInputWindow, stateFamily, state, scopedReadStateSupplier);
      sideInput = typed;
      if (sideInput != null) {
        tagCache.put(sideInputWindow, sideInput);
        return sideInput;
      } else {
        return null;
      }
    } else {
      return sideInput;
    }
  }

  public Iterable<Windmill.GlobalDataId> getSideInputNotifications() {
    return work.getGlobalDataIdNotificationsList();
  }

  public ByteString getSerializedKey() {
    return work.getKey();
  }

  public long getWorkToken() {
    return work.getWorkToken();
  }

  public Windmill.WorkItem getWork() {
    return work;
  }

  public Windmill.WorkItemCommitRequest.Builder getOutputBuilder() {
    return outputBuilder;
  }

  public UnboundedSource.UnboundedReader<?> getCachedReader() {
    ReaderCacheEntry entry = readerCache.get(getSerializedKey());
    if (entry == null) {
      return null;
    } else if (entry.token != getWork().getCacheToken()) {
      readerCache.remove(getSerializedKey());
      return null;
    } else {
      return entry.reader;
    }
  }

  public void setActiveReader(UnboundedSource.UnboundedReader<?> reader) {
    activeReader = reader;
  }

  public UnboundedSource.CheckpointMark getReaderCheckpoint(
      Coder<? extends UnboundedSource.CheckpointMark> coder) {
    try {
      ByteString state = work.getSourceState().getState();
      if (state.isEmpty()) {
        return null;
      }
      return coder.decode(state.newInput(), Coder.Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException("Exception while decoding checkpoint", e);
    }
  }

  public Map<Long, Runnable> flushState() {
    Map<Long, Runnable> callbacks = new HashMap<>();

    for (ExecutionContext.StepContext stepContext : getAllStepContexts()) {
      ((StepContext) stepContext).flushState();
    }


    if (activeReader != null) {
      Windmill.SourceState.Builder sourceStateBuilder =
          outputBuilder.getSourceStateUpdatesBuilder();
      final UnboundedSource.CheckpointMark checkpointMark = activeReader.getCheckpointMark();
      final Instant watermark = activeReader.getWatermark();
      long id = ThreadLocalRandom.current().nextLong();
      sourceStateBuilder.addFinalizeIds(id);
      callbacks.put(
          id,
          new Runnable() {
            @Override
            public void run() {
              try {
                checkpointMark.finalizeCheckpoint();
              } catch (IOException e) {
                throw new RuntimeException("Exception while finalizing checkpoint", e);
              }
            }
          });

      Coder<UnboundedSource.CheckpointMark> checkpointCoder =
          ((UnboundedSource<?, UnboundedSource.CheckpointMark>) activeReader.getCurrentSource())
              .getCheckpointMarkCoder();
      if (checkpointCoder != null) {
        ByteString.Output stream = ByteString.newOutput();
        try {
          checkpointCoder.encode(checkpointMark, stream, Coder.Context.OUTER);
        } catch (IOException e) {
          throw new RuntimeException("Exception while encoding checkpoint", e);
        }
        sourceStateBuilder.setState(stream.toByteString());
      }
      outputBuilder.setSourceWatermark(TimeUnit.MILLISECONDS.toMicros(watermark.getMillis()));

      long backlogBytes = activeReader.getSplitBacklogBytes();
      if (backlogBytes == UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN
          && CustomSources.isFirstUnboundedSourceSplit(getSerializedKey())) {
        // Only call getTotalBacklogBytes() on the first split.
        backlogBytes = activeReader.getTotalBacklogBytes();
      }
      if (backlogBytes != UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN) {
        outputBuilder.addCounterUpdates(
            Windmill.Counter.newBuilder()
            .setName("dataflow_backlog_size-" + stageName)
            .setKind(Windmill.Counter.Kind.SUM)
            .setIntScalar(backlogBytes)
            .setCumulative(true)
            .build());
      }

      readerCache.put(
          getSerializedKey(), new ReaderCacheEntry(activeReader, getWork().getCacheToken()));
    }
    return callbacks;
  }

  public List<Long> getReadyCommitCallbackIds() {
    return work.getSourceState().getFinalizeIdsList();
  }

  /**
   * Produce a tag that is guaranteed to be unique for the given namespace, domain and timestamp.
   *
   * <p>This is necessary because Windmill will deduplicate based only on this tag.
   */
  public static ByteString timerTag(TimerData key) {
    String tagString = String.format("%s+%d:%d",
        key.getNamespace().stringKey(), key.getDomain().ordinal(),
        key.getTimestamp().getMillis());
    return ByteString.copyFromUtf8(tagString);
  }

  private static class WindmillTimerInternals implements TimerInternals {

    private Map<TimerData, Boolean> timers = new HashMap<>();
    private Instant inputDataWatermark;
    private String stateFamily;

    public WindmillTimerInternals(String stateFamily, Instant inputDataWatermark) {
      this.inputDataWatermark = inputDataWatermark;
      this.stateFamily = stateFamily;
    }

    @Override
    public void setTimer(TimerData timerKey) {
      timers.put(timerKey, true);
    }

    @Override
    public void deleteTimer(TimerData timerKey) {
      timers.put(timerKey, false);
    }

    @Override
    public Instant currentProcessingTime() {
      return Instant.now();
    }

    @Override
    public Instant currentWatermarkTime() {
      return inputDataWatermark;
    }

    public void persistTo(Windmill.WorkItemCommitRequest.Builder outputBuilder) {
      for (Entry<TimerData, Boolean> entry : timers.entrySet()) {
        Windmill.Timer.Builder timer = outputBuilder.addOutputTimersBuilder()
            .setTag(timerTag(entry.getKey()))
            .setType(timerType(entry.getKey().getDomain()));
        if (stateFamily != null) {
          timer.setStateFamily(stateFamily);
        }

        // If the timer was being set (not deleted) then set a timestamp for it.
        if (entry.getValue()) {
          long timestampMicros =
              TimeUnit.MILLISECONDS.toMicros(entry.getKey().getTimestamp().getMillis());
          timer.setTimestamp(timestampMicros);
        }
      }
      timers.clear();
    }

    private Windmill.Timer.Type timerType(TimeDomain domain) {
      switch (domain) {
        case EVENT_TIME: return Windmill.Timer.Type.WATERMARK;
        case PROCESSING_TIME: return Windmill.Timer.Type.REALTIME;
        case SYNCHRONIZED_PROCESSING_TIME: return Windmill.Timer.Type.DEPENDENT_REALTIME;
        default:
          throw new IllegalArgumentException("Unrecgonized TimeDomain: " + domain);
      }
    }
  }

  class StepContext extends BaseExecutionContext.StepContext {
    private WindmillStateInternals stateInternals;
    private WindmillTimerInternals timerInternals;
    private final String prefix;
    private final String stateFamily;
    private final Supplier<StateSampler.ScopedState> scopedReadStateSupplier;

    public StepContext(
        final String stepName, String transformName, final StateSampler stateSampler) {
      super(StreamingModeExecutionContext.this, stepName, transformName);

      if (stateNameMap.isEmpty()) {
        this.prefix = transformName;
        this.stateFamily = "";
      } else {
        String mappedName = stateNameMap.get(transformName);
        this.prefix = mappedName == null ? "" : mappedName;
        this.stateFamily = prefix;
      }
      this.scopedReadStateSupplier = new Supplier<StateSampler.ScopedState>() {
        private int readState = -1;  // Uninitialized value.

        @Override
        public StateSampler.ScopedState get() {
          if (stateSampler == null) {
            return null;
          }
          if (readState == -1) {
            readState = stateSampler.stateForName(stepName + "-windmill-read", StateKind.FRAMEWORK);
          }
          return stateSampler.scopedState(readState);
        }
      };
    }

    /**
     * Update the {@code stateReader} used by this {@code StepContext}.
     */
    public void start(WindmillStateReader stateReader, Instant inputDataWatermark) {
      boolean useStateFamilies = !stateNameMap.isEmpty();
      this.stateInternals =
          new WindmillStateInternals(
              prefix, useStateFamilies, stateReader, scopedReadStateSupplier);
      this.timerInternals = new WindmillTimerInternals(
          stateFamily, Preconditions.checkNotNull(inputDataWatermark));
    }

    public void flushState() {
      stateInternals.persist(outputBuilder);
      timerInternals.persistTo(outputBuilder);
    }

    @Override
    public <T, W extends BoundedWindow> void writePCollectionViewData(
        TupleTag<?> tag,
        Iterable<WindowedValue<T>> data, Coder<Iterable<WindowedValue<T>>> dataCoder,
        W window, Coder<W> windowCoder) throws IOException {
      if (getSerializedKey().size() != 0) {
        throw new IllegalStateException("writePCollectionViewData must follow a Combine.globally");
      }

      ByteString.Output dataStream = ByteString.newOutput();
      dataCoder.encode(data, dataStream, Coder.Context.OUTER);

      ByteString.Output windowStream = ByteString.newOutput();
      windowCoder.encode(window, windowStream, Coder.Context.OUTER);

      Windmill.GlobalData.Builder builder = Windmill.GlobalData.newBuilder()
          .setDataId(
              Windmill.GlobalDataId.newBuilder()
              .setTag(tag.getId())
              .setVersion(windowStream.toByteString())
              .build())
          .setData(dataStream.toByteString());
      if (stateFamily != null) {
        builder.setStateFamily(stateFamily);
      }

      outputBuilder.addGlobalDataUpdates(builder.build());
    }

    /**
     * Fetch the given side input asynchronously and return true if it is present.
     */
    public boolean issueSideInputFetch(
        PCollectionView<?> view, BoundedWindow mainInputWindow, SideInputState state) {
      BoundedWindow sideInputWindow =
          view.getWindowingStrategyInternal().getWindowFn().getSideInputWindow(mainInputWindow);
      return fetchSideInput(view, sideInputWindow, stateFamily, state, scopedReadStateSupplier)
          != null;
    }

    /**
     * Note that there is data on the current key that is blocked on the given side input.
     */
    public void addBlockingSideInput(Windmill.GlobalDataRequest sideInput) {
      if (stateFamily != null) {
        sideInput =
            Windmill.GlobalDataRequest.newBuilder(sideInput).setStateFamily(stateFamily).build();
      }
      outputBuilder.addGlobalDataRequests(sideInput);
      outputBuilder.addGlobalDataIdRequests(sideInput.getDataId());
    }

    /**
     * Note that there is data on the current key that is blocked on the given side inputs.
     */
    public void addBlockingSideInputs(Iterable<Windmill.GlobalDataRequest> sideInputs) {
      for (Windmill.GlobalDataRequest sideInput : sideInputs) {
        addBlockingSideInput(sideInput);
      }
    }

    @Override
    public StateInternals stateInternals() {
      return Preconditions.checkNotNull(stateInternals);
    }

    @Override
    public TimerInternals timerInternals() {
      return Preconditions.checkNotNull(timerInternals);
    }
  }

  /**
   * A {@link SideInputReader} that fetches side inputs from the streaming worker's
   * cache.
   */
  public static class StreamingModeSideInputReader implements SideInputReader {
    private StreamingModeExecutionContext context;
    private Set<PCollectionView<?>> viewSet;

    private StreamingModeSideInputReader(
        Iterable<? extends PCollectionView<?>> views, StreamingModeExecutionContext context) {
      this.context = context;
      this.viewSet = ImmutableSet.copyOf(views);
    }

    public static StreamingModeSideInputReader of(
        Iterable<? extends PCollectionView<?>> views, StreamingModeExecutionContext context) {
      return new StreamingModeSideInputReader(views, context);
    }

    @Override
    public <T> T get(PCollectionView<T> view, BoundedWindow window) {
      if (!contains(view)) {
        throw new RuntimeException("get() called with unknown view");
      }

      // We are only fetching the cached value here, so we don't need stateFamily or
      // readStateSupplier.
      return context.fetchSideInput(view, window, null /* unused stateFamily */,
          SideInputState.CACHED_IN_WORKITEM, null /* unused readStateSupplier */);
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return viewSet.contains(view);
    }

    @Override
    public boolean isEmpty() {
      return viewSet.isEmpty();
    }
  }
}

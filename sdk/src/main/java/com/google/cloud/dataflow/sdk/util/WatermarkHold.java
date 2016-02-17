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

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.ClosingBehavior;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.WatermarkStateInternal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;

import javax.annotation.Nullable;

/**
 * Implements the logic to hold the output watermark for a computation back
 * until it has seen all the elements it needs based on the input watermark for the
 * computation.
 *
 * <p>The backend ensures the output watermark can never progress beyond the
 * input watermark for a computation. GroupAlsoByWindows computations may add a 'hold'
 * to the output watermark in order to prevent it progressing beyond a time within a window.
 * The hold will be 'cleared' when the associated pane is emitted.
 *
 * <p>This class is only intended for use by {@link ReduceFnRunner}. The two evolve together and
 * will likely break any other uses.
 *
 * @param <W> The kind of {@link BoundedWindow} the hold is for.
 */
public class WatermarkHold<W extends BoundedWindow> implements Serializable {
  /**
   * Return tag for state containing the output watermark hold
   * used for elements.
   */
  public static StateTag<WatermarkStateInternal> watermarkHoldTagForOutputTimeFn(
      OutputTimeFn<?> outputTimeFn) {
    return StateTags.makeSystemTagInternal(StateTags.watermarkStateInternal("hold", outputTimeFn));
  }

  /**
   * Tag for state containing end-of-window and garbage collection output watermark holds.
   * (We can't piggy-back on the data hold state since the outputTimeFn may be
   * {@link OutputTimeFns#outputAtLatestInputTimestamp()}, in which case every pane will
   * would take the end-of-window time as its element time.)
   */
  @VisibleForTesting
  public static final StateTag<WatermarkStateInternal> EXTRA_HOLD_TAG =
      StateTags.makeSystemTagInternal(StateTags.watermarkStateInternal(
          "extra", OutputTimeFns.outputAtEarliestInputTimestamp()));

  private final TimerInternals timerInternals;
  private final WindowingStrategy<?, W> windowingStrategy;
  private final StateTag<WatermarkStateInternal> elementHoldTag;

  public WatermarkHold(TimerInternals timerInternals, WindowingStrategy<?, W> windowingStrategy) {
    this.timerInternals = timerInternals;
    this.windowingStrategy = windowingStrategy;
    this.elementHoldTag = watermarkHoldTagForOutputTimeFn(windowingStrategy.getOutputTimeFn());
  }

  /**
   * Add a hold to prevent the output watermark progressing beyond the (possibly adjusted) timestamp
   * of the element in {@code context}. We allow the actual hold time to be shifted later by
   * {@link OutputTimeFn#assignOutputTime}, but no further than the end of the window. The hold will
   * remain until cleared by {@link #extractAndRelease}. Return the timestamp at which the hold
   * was placed, or {@literal null} if no hold was placed.
   *
   * <p>In the following we'll write {@code E} to represent an element's timestamp after passing
   * through the window strategy's output time function, {@code IWM} for the local input watermark,
   * {@code OWM} for the local output watermark, and {@code GCWM} for the garbage collection
   * watermark (which is at {@code IWM - getAllowedLateness}). Time progresses from left to right,
   * and we write {@code [ ... ]} to denote a bounded window with implied lower bound.
   *
   * <p>Note that the GCWM will be the same as the IWM if {@code getAllowedLateness}
   * is {@code ZERO}.
   *
   * <p>Here are the cases we need to handle. They are conceptually considered in the
   * sequence written since if getAllowedLateness is ZERO the GCWM is the same as the IWM.
   * <ol>
   * <li>(Normal)
   * <pre>
   *          |
   *      [   | E        ]
   *          |
   *         IWM
   * </pre>
   * This is, hopefully, the common and happy case. The element is locally on-time and can
   * definitely make it to an {@code ON_TIME} pane which we can still set an end-of-window timer
   * for. We place an element hold at E, which may contribute to the {@code ON_TIME} pane's
   * timestamp (depending on the output time function). Thus the OWM will not proceed past E
   * until the next pane fires.
   *
   * <li>(Discard - no target window)
   * <pre>
   *                       |                            |
   *      [     E        ] |                            |
   *                       |                            |
   *                     GCWM  <-getAllowedLateness->  IWM
   * </pre>
   * The element is very locally late. The window has been garbage collected, thus there
   * is no target pane E could be assigned to. We discard E.
   *
   * <li>(Unobservably late)
   * <pre>
   *          |    |
   *      [   | E  |     ]
   *          |    |
   *         OWM  IWM
   * </pre>
   * The element is locally late, however we can still treat this case as for 'Normal' above
   * since the IWM has not yet passed the end of the window and the element is ahead of the
   * OWM. In effect, we get to 'launder' the locally late element and consider it as locally
   * on-time because no downstream computation can observe the difference.
   *
   * <li>(Maybe late 1)
   * <pre>
   *          |            |
   *      [   | E        ] |
   *          |            |
   *         OWM          IWM
   * </pre>
   * The end-of-window timer may have already fired for this window, and thus an {@code ON_TIME}
   * pane may have already been emitted. However, if timer firings have been delayed then it
   * is possible the {@code ON_TIME} pane has not yet been emitted. We can't place an element
   * hold since we can't be sure if it will be cleared promptly. Thus this element *may* find
   * its way into an {@code ON_TIME} pane, but if so it will *not* contribute to that pane's
   * timestamp. We may however set a garbage collection hold if required.
   *
   * <li>(Maybe late 2)
   * <pre>
   *               |   |
   *      [     E  |   | ]
   *               |   |
   *              OWM IWM
   * </pre>
   * The end-of-window timer has not yet fired, so this element may still appear in an
   * {@code ON_TIME} pane. However the element is too late to contribute to the output
   * watermark hold, and thus won't contribute to the pane's timestamp. We can still place an
   * end-of-window hold.
   *
   * <li>(Maybe late 3)
   * <pre>
   *               |       |
   *      [     E  |     ] |
   *               |       |
   *              OWM     IWM
   * </pre>
   * As for the (Maybe late 2) case, however we don't even know if the end-of-window timer
   * has already fired, or it is about to fire. We can place only the garbage collection hold,
   * if required.
   *
   * <li>(Definitely late)
   * <pre>
   *                       |   |
   *      [     E        ] |   |
   *                       |   |
   *                      OWM IWM
   * </pre>
   * The element is definitely too late to make an {@code ON_TIME} pane. We are too late to
   * place an end-of-window hold. We can still place a garbage collection hold if required.
   *
   * </ol>
   */
  @Nullable
  public Instant addHolds(ReduceFn<?, ?, ?, W>.ProcessValueContext context) {
    Instant hold = addElementHold(context);
    if (hold == null) {
      hold = addEndOfWindowOrGarbageCollectionHolds(context, true);
    }
    return hold;
  }

  /**
   * Return {@code timestamp}, possibly shifted forward in time according to the window
   * strategy's output time function.
   */
  private Instant shift(Instant timestamp, W window) {
    Instant shifted = windowingStrategy.getOutputTimeFn().assignOutputTime(timestamp, window);
    if (shifted.isBefore(timestamp)) {
      throw new IllegalStateException(
          String.format("OutputTimeFn moved element from %s to earlier time %s for window %s",
              timestamp, shifted, window));
    }
    if (!timestamp.isAfter(window.maxTimestamp()) && shifted.isAfter(window.maxTimestamp())) {
      throw new IllegalStateException(
          String.format("OutputTimeFn moved element from %s to %s which is beyond end of window %s",
              timestamp, shifted, window));
    }

    return shifted;
  }

  /**
   * Add an element hold if possible. Return instant at which hold was added, or {@literal null}
   * if no hold was added.
   */
  @Nullable
  private Instant addElementHold(ReduceFn<?, ?, ?, W>.ProcessValueContext context) {
    // Give the window function a chance to move the hold timestamp forward to encourage progress.
    // (A later hold implies less impediment to the output watermark making progress, which in
    // turn encourages end-of-window triggers to fire earlier in following computations.)
    Instant elementHold = shift(context.timestamp(), context.window());

    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();

    // Only add the hold if we can be sure:
    // - the backend will be able to respect it
    // (ie the hold is at or ahead of the output watermark), AND
    // - a timer will be set to clear it by the end of window
    // (ie the end of window is at or ahead of the input watermark).
    String which;
    boolean tooLate;
    // TODO: These case labels could be tightened.
    // See the case analysis in addHolds above for the motivation.
    if (outputWM != null && elementHold.isBefore(outputWM)) {
      which = "too late to effect output watermark";
      tooLate = true;
    } else if (inputWM != null && context.window().maxTimestamp().isBefore(inputWM)) {
      which = "too late for end-of-window timer";
      tooLate = true;
    } else {
      which = "on time";
      tooLate = false;
      context.state().accessAcrossMergedWindows(elementHoldTag).add(elementHold);
    }
    WindowTracing.trace(
        "WatermarkHold.addHolds: element hold at {} is {} for "
        + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        elementHold, which, context.key(), context.window(), inputWM,
        outputWM);

    return tooLate ? null : elementHold;
  }

  /**
   * Add an end-of-window hold or, if too late for that, a garbage collection hold (if required).
   * Return the {@link Instant} at which hold was added, or {@literal null} if no hold was added.
   *
   * <p>The end-of-window hold guarantees that an empty {@code ON_TIME} pane can be given
   * a timestamp which will not be considered beyond allowed lateness by any downstream computation.
   */
  @Nullable
  private Instant addEndOfWindowOrGarbageCollectionHolds(ReduceFn<?, ?, ?, W>.Context context,
      boolean isActive) {
    Instant hold = addEndOfWindowHold(context, isActive);
    if (hold == null) {
      hold = addGarbageCollectionHold(context, isActive);
    }
    return hold;
  }

  /**
   * Add an end-of-window hold. Return the {@link Instant} at which hold was added,
   * or {@literal null} if no hold was added.
   *
   * <p>The end-of-window hold guarantees that any empty {@code ON_TIME} pane can be given
   * a timestamp which will not be considered beyond allowed lateness by any downstream computation.
   */
  @Nullable
  private Instant addEndOfWindowHold(ReduceFn<?, ?, ?, W>.Context context, boolean isActive) {
    // Only add an end-of-window hold if we can be sure a timer will be set to clear it
    // by the end of window (ie the end of window is at or ahead of the input watermark).
    Instant outputWM = timerInternals.currentOutputWatermarkTime();
    Instant inputWM = timerInternals.currentInputWatermarkTime();
    String which;
    boolean tooLate;
    Instant eowHold = context.window().maxTimestamp();
    if (inputWM != null && eowHold.isBefore(inputWM)) {
      which = "too late for end-of-window timer";
      tooLate = true;
    } else {
      which = "on time";
      tooLate = false;
      Preconditions.checkState(outputWM == null || !eowHold.isBefore(outputWM),
          "End-of-window hold %s cannot be before output watermark %s", eowHold, outputWM);
      if (isActive) {
        context.state().accessAcrossMergedWindows(EXTRA_HOLD_TAG).add(eowHold);
      } else {
        // The window is not currently ACTIVE, so we can't use accessAcrossMergedWindows
        // to collect its state. Instead, store the holds under the window itself. The
        // caller will be responsible for ensuring the active window set now considers this
        // window ACTIVE.
        context.state().access(EXTRA_HOLD_TAG).add(eowHold);
      }
    }
    WindowTracing.trace(
        "WatermarkHold.addEndOfWindowHold: end-of-window hold at {} is {} for "
        + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        eowHold, which, context.key(), context.window(), inputWM,
        outputWM);

    return tooLate ? null : eowHold;
  }

  /**
   * Add a garbage collection hold, if required. Return the {@link Instant} at which hold was added,
   * or {@literal null} if no hold was added.
   *
   * <p>The garbage collection hold gurantees that any empty final pane can be given
   * a timestamp which will not be considered beyond allowed lateness by any downstream
   * computation. If we are sure no empty final panes can be emitted then there's no need
   * for an additional hold.
   */
  @Nullable
  private Instant addGarbageCollectionHold(ReduceFn<?, ?, ?, W>.Context context,
      boolean isActive) {
    // Only add a garbage collection hold if we may need to emit an empty pane
    // at garbage collection time, and garbage collection time is strictly after the
    // end of window. (All non-empty panes will have holds at their output
    // time derived from their incoming elements and no additional hold is required.)
    if (context.windowingStrategy().getClosingBehavior() == ClosingBehavior.FIRE_ALWAYS
        && windowingStrategy.getAllowedLateness().isLongerThan(Duration.ZERO)) {
      Instant gcHold = context.window().maxTimestamp().plus(windowingStrategy.getAllowedLateness());
      Instant outputWM = timerInternals.currentOutputWatermarkTime();
      Instant inputWM = timerInternals.currentInputWatermarkTime();
      WindowTracing.trace(
          "WatermarkHold.addGarbageCollectionHold: garbage collection at {} hold for "
          + "key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
          gcHold, context.key(), context.window(), inputWM, outputWM);
      Preconditions.checkState(inputWM == null || !gcHold.isBefore(inputWM),
          "Garbage collection hold %s cannot be before input watermark %s", gcHold, inputWM);
      if (isActive) {
        context.state().accessAcrossMergedWindows(EXTRA_HOLD_TAG).add(gcHold);
      } else {
        // See comment above for addEndOfWindowHold.
        context.state().access(EXTRA_HOLD_TAG).add(gcHold);
      }
      return gcHold;
    } else {
      return null;
    }
  }

  /**
   * Updates the watermark hold when windows merge if it is possible the merged value does
   * not equal all of the existing holds. For example, if the new window implies a later
   * watermark hold, then earlier holds may be released.
   *
   * <p>Note that state may be left behind in merged windows.
   */
  public void onMerge(ReduceFn<?, ?, ?, W>.OnMergeContext context) {
    WindowTracing.debug("onMerge: for key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        context.key(), context.window(), timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    // If the output hold depends only on the window, then there may not be a hold in place
    // for the new merged window, so add one.
    if (windowingStrategy.getOutputTimeFn().dependsOnlyOnWindow()) {
      Instant arbitraryTimestamp = new Instant(0);
      context.state()
          .accessAcrossMergedWindows(elementHoldTag)
          .add(windowingStrategy.getOutputTimeFn().assignOutputTime(
              arbitraryTimestamp, context.window()));
    }

    context.state().accessAcrossMergedWindows(elementHoldTag).releaseExtraneousHolds();
    context.state().accessAcrossMergedWindows(EXTRA_HOLD_TAG).releaseExtraneousHolds();
  }

  /**
   * Return (a future for) the earliest hold for {@code context}. Clear all the holds after
   * reading, but add/restore an end-of-window or garbage collection hold if required.
   *
   * <p>The returned timestamp is the output timestamp according to the {@link OutputTimeFn}
   * from the windowing strategy of this {@link WatermarkHold}, combined across all the non-late
   * elements in the current pane. If there is no such value the timestamp is the end
   * of the window.
   *
   * <p>If {@code willStillBeActive} then any end-of-window or garbage collection holds will
   * be reestablished in one of the target windows alread in use for this window. Otherwise,
   * the holds will be placed in this window itself.
   */
  public StateContents<Instant> extractAndRelease(final ReduceFn<?, ?, ?, W>.Context context,
      final boolean isFinal, final boolean willStillBeActive) {
    WindowTracing.debug(
        "extractAndRelease: for key:{}; window:{}; inputWatermark:{}; outputWatermark:{}",
        context.key(), context.window(), timerInternals.currentInputWatermarkTime(),
        timerInternals.currentOutputWatermarkTime());
    final WatermarkStateInternal elementHoldState =
        context.state().accessAcrossMergedWindows(elementHoldTag);
    // Since we only extract holds when a trigger fires it is unreasonable to expect
    // the state to be prefetched.
    final StateContents<Instant> elementHoldFuture = elementHoldState.get();
    final WatermarkStateInternal extraHoldState =
        context.state().accessAcrossMergedWindows(EXTRA_HOLD_TAG);
    final StateContents<Instant> extraHoldFuture = extraHoldState.get();
    return new StateContents<Instant>() {
      @Override
      public Instant read() {
        // Read both the element and extra holds.
        Instant elementHold = elementHoldFuture.read();
        Instant extraHold = extraHoldFuture.read();
        Instant hold;
        // Find the minimum, accounting for null.
        if (elementHold == null) {
          hold = extraHold;
        } else if (extraHold == null) {
          hold = elementHold;
        } else if (elementHold.isBefore(extraHold)) {
          hold = elementHold;
        } else {
          hold = extraHold;
        }
        if (hold == null || hold.isAfter(context.window().maxTimestamp())) {
          // If no hold (eg because all elements came in behind the output watermark), or
          // the hold was for garbage collection, take the end of window as the result.
          WindowTracing.debug(
              "WatermarkHold.extractAndRelease.read: clipping from {} to end of window "
              + "for key:{}; window:{}",
              hold, context.key(), context.window());
          hold = context.window().maxTimestamp();
        }
        WindowTracing.debug("WatermarkHold.extractAndRelease.read: clearing for key:{}; window:{}",
            context.key(), context.window());

        // Clear the underlying state to allow the output watermark to progress.
        elementHoldState.clear();
        extraHoldState.clear();

        if (!isFinal) {
          addEndOfWindowOrGarbageCollectionHolds(context, willStillBeActive);
        }

        return hold;
      }
    };
  }

  /**
   * Clear any remaining holds. If {@code isActive} then we assume holds could be placed in any
   * of the target windows for this window. Otherwise we assume only this window has any
   * end-of-window or garbage collection holds.
   */
  public void clearHolds(ReduceFn<?, ?, ?, W>.Context context, boolean isActive) {
    WindowTracing.debug(
        "WatermarkHold.clearHolds: For key:{}; %s window:{}; "
        + "inputWatermark:{}; outputWatermark:{}",
        context.key(), isActive ? "active" : "inactive", context.window(),
        timerInternals.currentInputWatermarkTime(), timerInternals.currentOutputWatermarkTime());
    if (isActive) {
      context.state().accessAcrossMergedWindows(elementHoldTag).clear();
      context.state().accessAcrossMergedWindows(EXTRA_HOLD_TAG).clear();
    } else {
      context.state().access(EXTRA_HOLD_TAG).clear();
    }
  }
}

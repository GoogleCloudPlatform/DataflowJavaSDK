package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.testing.TestStream;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TestStreamEvaluatorFactory}. */
@RunWith(JUnit4.class)
public class TestStreamEvaluatorFactoryTest {
  private TestStreamEvaluatorFactory factory = new TestStreamEvaluatorFactory();
  private BundleFactory bundleFactory = InProcessBundleFactory.create();

  /** Demonstrates that returned evaluators produce elements in sequence. */
  @Test
  public void producesElementsInSequence() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> streamVals =
        p.apply(
            TestStream.create(VarIntCoder.of())
                .addElements(1, 2, 3)
                .addElements(4, 5, 6)
                .advanceWatermarkToInfinity());

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    when(context.createRootBundle(streamVals))
        .thenReturn(
            bundleFactory.createRootBundle(streamVals), bundleFactory.createRootBundle(streamVals));

    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null, context);
    InProcessTransformResult firstResult = firstEvaluator.finishBundle();

    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null, context);
    InProcessTransformResult secondResult = secondEvaluator.finishBundle();

    TransformEvaluator<Object> thirdEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null, context);
    InProcessTransformResult thirdResult = thirdEvaluator.finishBundle();

    assertThat(
        Iterables.getOnlyElement(firstResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(1),
            WindowedValue.valueInGlobalWindow(2),
            WindowedValue.valueInGlobalWindow(3)));
    assertThat(firstResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    assertThat(
        Iterables.getOnlyElement(secondResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(
            WindowedValue.valueInGlobalWindow(4),
            WindowedValue.valueInGlobalWindow(5),
            WindowedValue.valueInGlobalWindow(6)));
    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    assertThat(Iterables.isEmpty(thirdResult.getOutputBundles()), is(true));
    assertThat(thirdResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MAX_VALUE));
  }

  /** Demonstrates that at most one evaluator for an application is available at a time. */
  @Test
  public void onlyOneEvaluatorAtATime() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> streamVals =
        p.apply(
            TestStream.create(VarIntCoder.of()).addElements(4, 5, 6).advanceWatermarkToInfinity());

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null, context);

    // create a second evaluator before the first is finished. The evaluator should not be available
    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(streamVals.getProducingTransformInternal(), null, context);
    assertThat(secondEvaluator, is(nullValue()));
  }

  /**
   * Demonstrates that multiple applications of the same {@link TestStream} produce separate
   * evaluators.
   */
  @Test
  public void multipleApplicationsMultipleEvaluators() throws Exception {
    TestPipeline p = TestPipeline.create();
    TestStream<Integer> stream =
        TestStream.create(VarIntCoder.of()).addElements(2).advanceWatermarkToInfinity();
    PCollection<Integer> firstVals = p.apply("Stream One", stream);
    PCollection<Integer> secondVals = p.apply("Stream A", stream);

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    when(context.createRootBundle(firstVals)).thenReturn(bundleFactory.createRootBundle(firstVals));
    when(context.createRootBundle(secondVals))
        .thenReturn(bundleFactory.createRootBundle(secondVals));

    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(firstVals.getProducingTransformInternal(), null, context);
    // The two evaluators can exist independently
    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(secondVals.getProducingTransformInternal(), null, context);

    InProcessTransformResult firstResult = firstEvaluator.finishBundle();
    InProcessTransformResult secondResult = secondEvaluator.finishBundle();

    assertThat(
        Iterables.getOnlyElement(firstResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow(2)));
    assertThat(firstResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    // They both produce equal results, and don't interfere with each other
    assertThat(
        Iterables.getOnlyElement(secondResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow(2)));
    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }

  /**
   * Demonstrates that multiple applications of different {@link TestStream} produce independent
   * evaluators.
   */
  @Test
  public void multipleStreamsMultipleEvaluators() throws Exception {
    TestPipeline p = TestPipeline.create();
    PCollection<Integer> firstVals =
        p.apply(
            "Stream One",
            TestStream.create(VarIntCoder.of()).addElements(2).advanceWatermarkToInfinity());
    PCollection<String> secondVals =
        p.apply(
            "Stream A",
            TestStream.create(StringUtf8Coder.of())
                .addElements("Two")
                .advanceWatermarkToInfinity());

    InProcessEvaluationContext context = mock(InProcessEvaluationContext.class);
    when(context.createRootBundle(firstVals)).thenReturn(bundleFactory.createRootBundle(firstVals));
    when(context.createRootBundle(secondVals))
        .thenReturn(bundleFactory.createRootBundle(secondVals));

    TransformEvaluator<Object> firstEvaluator =
        factory.forApplication(firstVals.getProducingTransformInternal(), null, context);
    // The two evaluators can exist independently
    TransformEvaluator<Object> secondEvaluator =
        factory.forApplication(secondVals.getProducingTransformInternal(), null, context);

    InProcessTransformResult firstResult = firstEvaluator.finishBundle();
    InProcessTransformResult secondResult = secondEvaluator.finishBundle();

    assertThat(
        Iterables.getOnlyElement(firstResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow(2)));
    assertThat(firstResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));

    assertThat(
        Iterables.getOnlyElement(secondResult.getOutputBundles())
            .commit(Instant.now())
            .getElements(),
        Matchers.<WindowedValue<?>>containsInAnyOrder(WindowedValue.valueInGlobalWindow("Two")));
    assertThat(secondResult.getWatermarkHold(), equalTo(BoundedWindow.TIMESTAMP_MIN_VALUE));
  }
}


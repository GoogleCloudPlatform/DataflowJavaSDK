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

import static com.google.cloud.dataflow.sdk.util.Structs.getBytes;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.WriteInstruction;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.AppliedCombineFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObservable;
import com.google.cloud.dataflow.sdk.util.common.ElementByteSizeObserver;
import com.google.cloud.dataflow.sdk.util.common.worker.ElementCounter;
import com.google.cloud.dataflow.sdk.util.common.worker.FlattenOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.MapTaskExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.Operation;
import com.google.cloud.dataflow.sdk.util.common.worker.OutputReceiver;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.PartialGroupByKeyOperation.GroupingKeyCreator;
import com.google.cloud.dataflow.sdk.util.common.worker.ReadOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.ReceivingOperation;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.common.worker.WriteOperation;
import com.google.cloud.dataflow.sdk.values.KV;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Creates a MapTaskExecutor from a MapTask definition.
 */
public class MapTaskExecutorFactory {

  /**
   * Creates a new MapTaskExecutor from the given MapTask definition using the default
   * {@link ReaderFactory.Registry}.
   */
  public static MapTaskExecutor create(
      PipelineOptions options,
      MapTask mapTask,
      DataflowExecutionContext context,
      CounterSet counters,
      StateSampler stateSampler) throws Exception {
    return create(
        options,
        mapTask,
        ReaderFactory.Registry.defaultRegistry(),
        context,
        counters,
        stateSampler);
  }

  /**
   * Creates a new MapTaskExecutor from the given MapTask definition using the provided
   * {@link ReaderFactory.Registry}.
   */
  public static MapTaskExecutor create(
      PipelineOptions options,
      MapTask mapTask,
      ReaderFactory.Registry registry,
      DataflowExecutionContext context,
      CounterSet counters,
      StateSampler stateSampler)
          throws Exception {

    List<Operation> operations = new ArrayList<>();
    String counterPrefix = stateSampler.getPrefix();

    // Instantiate operations for each instruction in the graph.
    for (ParallelInstruction instruction : mapTask.getInstructions()) {
      operations.add(createOperation(
          options,
          instruction,
          registry,
          context,
          operations,
          counterPrefix,
          counters.getAddCounterMutator(),
          stateSampler));
    }

    return new MapTaskExecutor(operations, counters, stateSampler);

  }

  /**
   * Creates an Operation from the given ParallelInstruction definition using the provided
   * {@link ReaderFactory.Registry}.
   */
  static Operation createOperation(
      PipelineOptions options,
      ParallelInstruction instruction,
      DataflowExecutionContext executionContext,
      List<Operation> priorOperations,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler)
          throws Exception {
    return createOperation(
        options,
        instruction,
        ReaderFactory.Registry.defaultRegistry(),
        executionContext,
        priorOperations,
        counterPrefix,
        addCounterMutator,
        stateSampler);
  }

  /**
   * Creates an Operation from the given ParallelInstruction definition using the provided
   * {@link ReaderFactory.Registry}.
   */
  static Operation createOperation(
      PipelineOptions options,
      ParallelInstruction instruction,
      ReaderFactory.Registry registry,
      DataflowExecutionContext executionContext,
      List<Operation> priorOperations,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler)
          throws Exception {
    if (instruction.getRead() != null) {
      return createReadOperation(
          options,
          instruction,
          registry,
          executionContext,
          priorOperations,
          counterPrefix,
          addCounterMutator,
          stateSampler);
    } else if (instruction.getWrite() != null) {
      return createWriteOperation(options, instruction, executionContext, priorOperations,
          counterPrefix, addCounterMutator, stateSampler);
    } else if (instruction.getParDo() != null) {
      return createParDoOperation(options, instruction, executionContext, priorOperations,
          counterPrefix, addCounterMutator, stateSampler);
    } else if (instruction.getPartialGroupByKey() != null) {
      return createPartialGroupByKeyOperation(options, instruction, executionContext,
          priorOperations, counterPrefix, addCounterMutator, stateSampler);
    } else if (instruction.getFlatten() != null) {
      return createFlattenOperation(options, instruction, executionContext, priorOperations,
          counterPrefix, addCounterMutator, stateSampler);
    } else {
      throw new Exception("Unexpected instruction: " + instruction);
    }
  }

  static ReadOperation createReadOperation(
      PipelineOptions options,
      ParallelInstruction instruction,
      ReaderFactory.Registry registry,
      DataflowExecutionContext executionContext,
      @SuppressWarnings("unused") List<Operation> priorOperations,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler)
      throws Exception {
    ReadInstruction read = instruction.getRead();

    String operationName = instruction.getSystemName();
    Reader<?> reader = registry.create(
        read.getSource(), options, executionContext, addCounterMutator, operationName);

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 1);

    return new ReadOperation(operationName, reader, receivers, counterPrefix,
        addCounterMutator, stateSampler);
  }

  static WriteOperation createWriteOperation(PipelineOptions options,
      ParallelInstruction instruction, ExecutionContext executionContext,
      List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    WriteInstruction write = instruction.getWrite();

    Sink<?> sink =
        SinkFactory.create(options, write.getSink(), executionContext, addCounterMutator);

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 0);

    WriteOperation operation = new WriteOperation(instruction.getSystemName(), sink, receivers,
        counterPrefix, addCounterMutator, stateSampler);

    attachInput(operation, write.getInput(), priorOperations);

    return operation;
  }

  private static ParDoFnFactory parDoFnFactory = new DefaultParDoFnFactory();

  static ParDoOperation createParDoOperation(
      PipelineOptions options,
      ParallelInstruction instruction,
      DataflowExecutionContext executionContext,
      List<Operation> priorOperations,
      String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler)
      throws Exception {
    ParDoInstruction parDo = instruction.getParDo();

    ParDoFn fn = parDoFnFactory.create(
        options,
        CloudObject.fromSpec(parDo.getUserFn()),
        instruction.getSystemName(),
        instruction.getName(),
        parDo.getSideInputs(),
        parDo.getMultiOutputInfos(),
        parDo.getNumOutputs(),
        executionContext,
        addCounterMutator,
        stateSampler);

    OutputReceiver[] receivers = createOutputReceivers(
        instruction, counterPrefix, addCounterMutator, stateSampler, parDo.getNumOutputs());

    ParDoOperation operation = new ParDoOperation(
        instruction.getSystemName(), fn, receivers, counterPrefix, addCounterMutator, stateSampler);

    attachInput(operation, parDo.getInput(), priorOperations);

    return operation;
  }

  static PartialGroupByKeyOperation createPartialGroupByKeyOperation(
      @SuppressWarnings("unused") PipelineOptions options,
      ParallelInstruction instruction,
      @SuppressWarnings("unused") ExecutionContext executionContext,
      List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    PartialGroupByKeyInstruction pgbk = instruction.getPartialGroupByKey();

    Coder<?> windowedCoder = Serializer.deserialize(pgbk.getInputElementCodec(), Coder.class);
    if (!(windowedCoder instanceof WindowedValueCoder)) {
      throw new Exception(
          "unexpected kind of input coder for PartialGroupByKeyOperation: " + windowedCoder);
    }
    Coder<?> elemCoder = ((WindowedValueCoder<?>) windowedCoder).getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new Exception(
          "unexpected kind of input element coder for PartialGroupByKeyOperation: " + elemCoder);
    }
    KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) elemCoder;
    Coder<?> keyCoder = kvCoder.getKeyCoder();
    Coder<?> valueCoder = kvCoder.getValueCoder();

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 1);

    PartialGroupByKeyOperation.Combiner<?, ?, ?, ?> valueCombiner = createValueCombiner(pgbk);

    PartialGroupByKeyOperation operation = new PartialGroupByKeyOperation(
        instruction.getSystemName(),
        new WindowingCoderGroupingKeyCreator<>(keyCoder),
        new CoderSizeEstimator<>(WindowedValue.getValueOnlyCoder(keyCoder)),
        new CoderSizeEstimator<>(valueCoder), 0.001 /*sizeEstimatorSampleRate*/, valueCombiner,
        PairInfo.create(), receivers, counterPrefix, addCounterMutator, stateSampler);

    attachInput(operation, pgbk.getInput(), priorOperations);

    return operation;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static ValueCombiner createValueCombiner(PartialGroupByKeyInstruction pgbk) throws Exception {
    if (pgbk.getValueCombiningFn() == null) {
      return null;
    }

    Object deserializedFn = SerializableUtils.deserializeFromByteArray(
        getBytes(CloudObject.fromSpec(pgbk.getValueCombiningFn()), PropertyNames.SERIALIZED_FN),
        "serialized combine fn");
    return new ValueCombiner(((AppliedCombineFn) deserializedFn).getFn());
  }

  /**
   * Implements PGBKOp.Combiner via Combine.KeyedCombineFn.
   */
  public static class ValueCombiner<K, InputT, AccumT, OutputT>
      implements PartialGroupByKeyOperation.Combiner<WindowedValue<K>, InputT, AccumT, OutputT> {
    private final Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn;

    private ValueCombiner(Combine.KeyedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
      this.combineFn = combineFn;
    }

    @Override
    public AccumT createAccumulator(WindowedValue<K> windowedKey) {
      return this.combineFn.createAccumulator(windowedKey.getValue());
    }

    @Override
    public AccumT add(WindowedValue<K> windowedKey, AccumT accumulator, InputT value) {
      return this.combineFn.addInput(windowedKey.getValue(), accumulator, value);
    }

    @Override
    public AccumT merge(WindowedValue<K> windowedKey, Iterable<AccumT> accumulators) {
      return this.combineFn.mergeAccumulators(windowedKey.getValue(), accumulators);
    }

    @Override
    public OutputT extract(WindowedValue<K> windowedKey, AccumT accumulator) {
      return this.combineFn.extractOutput(windowedKey.getValue(), accumulator);
    }
  }

  /**
   * Implements PGBKOp.PairInfo via KVs.
   */
  public static class PairInfo implements PartialGroupByKeyOperation.PairInfo {
    private static PairInfo theInstance = new PairInfo();
    public static PairInfo create() {
      return theInstance;
    }
    private PairInfo() {}
    @Override
    public Object getKeyFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.withValue(windowedKv.getValue().getKey());
    }
    @Override
    public Object getValueFromInputPair(Object pair) {
      @SuppressWarnings("unchecked")
      WindowedValue<KV<?, ?>> windowedKv = (WindowedValue<KV<?, ?>>) pair;
      return windowedKv.getValue().getValue();
    }
    @Override
    public Object makeOutputPair(Object key, Object values) {
      WindowedValue<?> windowedKey = (WindowedValue<?>) key;
      return windowedKey.withValue(KV.of(windowedKey.getValue(), values));
    }
  }

  /**
   * Implements PGBKOp.GroupingKeyCreator via Coder.
   */
  // TODO: Actually support window merging in the combiner table.
  public static class WindowingCoderGroupingKeyCreator<K>
      implements GroupingKeyCreator<WindowedValue<K>> {

    private static final Instant ignored = BoundedWindow.TIMESTAMP_MIN_VALUE;

    private final Coder<K> coder;

    public WindowingCoderGroupingKeyCreator(Coder<K> coder) {
      this.coder = coder;
    }

    @Override
    public Object createGroupingKey(WindowedValue<K> key) throws Exception {
      // Ignore timestamp for grouping purposes.
      // The PGBK output will inherit the timestamp of one of its inputs.
      return WindowedValue.of(
          coder.structuralValue(key.getValue()),
          ignored,
          key.getWindows(),
          key.getPane());
    }
  }

  /**
   * Implements PGBKOp.SizeEstimator via Coder.
   */
  public static class CoderSizeEstimator<T>implements PartialGroupByKeyOperation.SizeEstimator<T> {
    final Coder<T> coder;

    public CoderSizeEstimator(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public long estimateSize(T value) throws Exception {
      return CoderUtils.encodeToByteArray(coder, value).length;
    }
  }

  static FlattenOperation createFlattenOperation(
      @SuppressWarnings("unused") PipelineOptions options,
      ParallelInstruction instruction,
      @SuppressWarnings("unused") ExecutionContext executionContext,
      List<Operation> priorOperations, String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator, StateSampler stateSampler) throws Exception {
    FlattenInstruction flatten = instruction.getFlatten();

    OutputReceiver[] receivers =
        createOutputReceivers(instruction, counterPrefix, addCounterMutator, stateSampler, 1);

    FlattenOperation operation = new FlattenOperation(
        instruction.getSystemName(), receivers, counterPrefix, addCounterMutator, stateSampler);

    for (InstructionInput input : flatten.getInputs()) {
      attachInput(operation, input, priorOperations);
    }

    return operation;
  }

  /**
   * Returns an array of OutputReceivers for the given
   * ParallelInstruction definition.
   */
  static OutputReceiver[] createOutputReceivers(ParallelInstruction instruction,
      @SuppressWarnings("unused") String counterPrefix,
      CounterSet.AddCounterMutator addCounterMutator,
      @SuppressWarnings("unused") StateSampler stateSampler,
      int expectedNumOutputs) throws Exception {
    int numOutputs = 0;
    if (instruction.getOutputs() != null) {
      numOutputs = instruction.getOutputs().size();
    }
    if (numOutputs != expectedNumOutputs) {
      throw new AssertionError("ParallelInstruction.Outputs has an unexpected length");
    }
    OutputReceiver[] receivers = new OutputReceiver[numOutputs];
    for (int i = 0; i < numOutputs; i++) {
      InstructionOutput cloudOutput = instruction.getOutputs().get(i);
      receivers[i] = new OutputReceiver();

      @SuppressWarnings("unchecked")
      ElementCounter outputCounter = new DataflowOutputCounter(
          cloudOutput.getName(),
          new ElementByteSizeObservableCoder<>(
              Serializer.deserialize(cloudOutput.getCodec(), Coder.class)),
          addCounterMutator);
      receivers[i].addOutputCounter(outputCounter);
    }
    return receivers;
  }

  /**
   * Adapts a Coder to the ElementByteSizeObservable interface.
   */
  public static class ElementByteSizeObservableCoder<T> implements ElementByteSizeObservable<T> {
    final Coder<T> coder;

    public ElementByteSizeObservableCoder(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(T value) {
      return coder.isRegisterByteSizeObserverCheap(value, Coder.Context.OUTER);
    }

    @Override
    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer)
        throws Exception {
      coder.registerByteSizeObserver(value, observer, Coder.Context.OUTER);
    }
  }

  /**
   * Adds an input to the given Operation, coming from the given
   * producer instruction output.
   */
  static void attachInput(ReceivingOperation operation, @Nullable InstructionInput input,
      List<Operation> priorOperations) {
    Integer producerInstructionIndex = 0;
    Integer outputNum = 0;
    if (input != null) {
      if (input.getProducerInstructionIndex() != null) {
        producerInstructionIndex = input.getProducerInstructionIndex();
      }
      if (input.getOutputNum() != null) {
        outputNum = input.getOutputNum();
      }
    }
    // Input id must refer to an operation that has already been seen.
    Operation source = priorOperations.get(producerInstructionIndex);
    operation.attachInput(source, outputNum);
  }
}


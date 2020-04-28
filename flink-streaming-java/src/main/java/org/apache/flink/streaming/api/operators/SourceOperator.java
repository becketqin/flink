/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Base source operator only used for integrating the source reader which is proposed by FLIP-27. It implements
 * the interface of {@link PushingAsyncDataInput} for naturally compatible with one input processing in runtime
 * stack.
 *
 * <p>Note: We are expecting this to be changed to the concrete class once SourceReader interface is introduced.
 *
 * @param <OUT> The output type of the operator.
 */
@Internal
public class SourceOperator<OUT, SplitT extends SourceSplit>
		extends AbstractStreamOperator<OUT>
		implements CoordinatedOperator, PushingAsyncDataInput<OUT> {
	// Package private for unit test.
	static final ListStateDescriptor<byte[]> SPLITS_STATE_DESC =
			new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE);
	static final ListStateDescriptor<Integer> SERIALIZER_VERSION_DESC =
			new ListStateDescriptor<>("SplitSerializerVersion", Integer.class);

	private final Source<OUT, SplitT, ?> source;
	private final int numWorkerThread;

	// Fields that will be setup at runtime.
	private transient SourceReader<OUT, SplitT> sourceReader;
	private transient SimpleVersionedSerializer<SplitT> splitSerializer;
	private transient ListState<Integer> serializerVersion;
	private transient ListState<byte[]> readerState;
	private transient OperatorEventGateway operatorEventGateway;

	public SourceOperator(Source<OUT, SplitT, ?> source) {
		this(source, 1);
	}

	public SourceOperator(Source<OUT, SplitT, ?> source, int numWorkerThread) {
		this.source = source;
		this.numWorkerThread = numWorkerThread;
	}

	@Override
	public void open() throws Exception {
		splitSerializer = source.getSplitSerializer();
		// Create the source reader.
		SourceReaderContext context = new SourceReaderContext() {
			@Override
			public MetricGroup metricGroup() {
				return getRuntimeContext().getMetricGroup();
			}

			@Override
			public void sendSourceEventToCoordinator(SourceEvent event) {
				operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
			}
		};
		sourceReader = source.createReader(context);

		// restore the state if necessary.
		if (readerState.get() != null && readerState.get().iterator().hasNext()) {
			Integer version = serializerVersion.get().iterator().next();
			List<SplitT> splits = new ArrayList<>();
			for (byte[] splitBytes : readerState.get()) {
				splits.add(splitSerializer.deserialize(version, splitBytes));
			}
			sourceReader.addSplits(splits);
		}
		serializerVersion.update(Collections.singletonList(splitSerializer.getVersion()));
		// Start the reader.
		sourceReader.start();
		// Register the reader to the coordinator.
		registerReader();
	}

	@Override
	@SuppressWarnings("unchecked")
	public InputStatus emitNext(DataOutput<OUT> output) throws Exception {
		switch (sourceReader.pollNext((SourceOutput<OUT>) output)) {
			case AVAILABLE_NOW:
				return InputStatus.MORE_AVAILABLE;
			case AVAILABLE_LATER:
				return InputStatus.NOTHING_AVAILABLE;
			case FINISHED:
				return InputStatus.END_OF_INPUT;
			default:
				throw new IllegalStateException("Should never reach here");
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		LOG.debug("Taking a snapshot for checkpoint {}", context.getCheckpointId());
		List<SplitT> splitStates = sourceReader.snapshotState();
		List<byte[]> state = new ArrayList<>();
		for (SplitT splitState : splitStates) {
			state.add(splitSerializer.serialize(splitState));
		}
		readerState.update(state);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return sourceReader.isAvailable();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		readerState = context.getOperatorStateStore().getListState(SPLITS_STATE_DESC);
		serializerVersion = context.getOperatorStateStore().getListState(SERIALIZER_VERSION_DESC);
	}

	@Override
	public OperatorCoordinator.Provider getCoordinatorProvider(OperatorID operatorID) {
		return new SourceCoordinatorProvider<>(operatorID, source, numWorkerThread);
	}

	public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
		this.operatorEventGateway = operatorEventGateway;
	}

	@SuppressWarnings("unchecked")
	public void handleOperatorEvent(OperatorEvent event) {
		if (event instanceof AddSplitEvent) {
			sourceReader.addSplits(((AddSplitEvent<SplitT>) event).splits());
		} else if (event instanceof SourceEventWrapper) {
			sourceReader.handleSourceEvents(((SourceEventWrapper) event).getSourceEvent());
		} else {
			throw new IllegalStateException("Received unexpected operator event " + event);
		}
	}

	private void registerReader() {
		operatorEventGateway.sendEventToCoordinator(new ReaderRegistrationEvent(
				getRuntimeContext().getIndexOfThisSubtask(),
				"UNKNOWN_LOCATION"));
	}

	// --------------- methods for unit tests ------------

	@VisibleForTesting
	public SourceReader<OUT, SplitT> getSourceReader() {
		return sourceReader;
	}
}

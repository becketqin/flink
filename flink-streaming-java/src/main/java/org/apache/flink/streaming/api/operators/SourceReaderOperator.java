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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connectors.source.Source;
import org.apache.flink.api.connectors.source.SourceOutput;
import org.apache.flink.api.connectors.source.SourceReader;
import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.event.AddSplitEvent;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.ReaderRegistrationEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.impl.connector.source.reader.SourceReaderContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.jobgraph.tasks.SourceCoordinatorDelegate;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.runtime.io.InputStatus;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Base source operator only used for integrating the source reader which is proposed by FLIP-27. It implements
 * the interface of {@link PushingAsyncDataInput} for naturally compatible with one input processing in runtime
 * stack.
 *
 * <p>Note: We are expecting this to be changed to the concrete class once SourceReader interface is introduced.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class SourceReaderOperator<OUT, SplitT extends SourceSplit>
		extends AbstractStreamOperator<OUT> implements PushingAsyncDataInput<OUT> {

	private final Source<OUT, SplitT, ?> source;
	private SourceReader<OUT, SplitT> sourceReader;
	private SimpleVersionedSerializer<SplitT> splitSerializer;
	private ValueState<Integer> serializerVersion;
	private ListState<byte[]> readerState;
	private SourceCoordinatorDelegate sourceCoordinatorDelegate;

	public SourceReaderOperator(Source<OUT, SplitT, ?> source) {
		this.source = source;
		this.splitSerializer = source.getSplitSerializer();
	}

	@Override
	public void open() throws Exception {
		sourceReader = source.createReader(getOperatorConfig().getConfiguration());
		sourceReader.setSourceReaderContext(new SourceReaderContext() {
			@Override
			public MetricGroup getMetricGroup() {
				return getRuntimeContext().getMetricGroup();
			}

			@Override
			public CompletableFuture<Void> sendSourceEvent(SourceEvent event) {
				return sourceCoordinatorDelegate.sendOperatorEvent(event);
			}
		});
		sourceReader.start();

		// restore the state if necessary.
		if (readerState.get() != null) {
			Integer version = serializerVersion.value();
			List<SplitT> splits = new ArrayList<>();
			for (byte[] splitBytes : readerState.get()) {
				splits.add(splitSerializer.deserialize(version, splitBytes));
			}
			sourceReader.addSplits(splits);
		}
		serializerVersion.update(splitSerializer.getVersion());
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
		List<SplitT> splitStates = sourceReader.snapshotState();
		List<byte[]> state = new ArrayList<>();
		for (SplitT splitState : splitStates) {
			state.add(splitSerializer.serialize(splitState));
		}
		readerState.update(state);
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		return sourceReader.isAvailable();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		readerState = getRuntimeContext().getListState(
				new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE));
		serializerVersion = getRuntimeContext().getState(
				new ValueStateDescriptor<>("SplitSerializerVersion", Integer.class));
	}

	public void setSourceCoordinatorDelegate(SourceCoordinatorDelegate sourceCoordinatorDelegate) {
		this.sourceCoordinatorDelegate = sourceCoordinatorDelegate;
	}

	@SuppressWarnings("unchecked")
	public void handleOperatorEvents(OperatorEvent event) {
		if (event instanceof AddSplitEvent) {
			sourceReader.addSplits(((AddSplitEvent<SplitT>) event).splits());
		} else if (event instanceof SourceEvent) {
			sourceReader.handleSourceEvents((SourceEvent) event);
		} else {
			throw new IllegalStateException("Received unexpected operator event " + event);
		}
	}

	public Source getSource() {
		return source;
	}

	private void registerReader() {
		sourceCoordinatorDelegate.sendOperatorEvent(new ReaderRegistrationEvent(
				getRuntimeContext().getIndexOfThisSubtask(),
				"Some Location"));
	}
}

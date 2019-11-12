/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connectors.source.ReaderInfo;
import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.SplitEnumeratorContext;
import org.apache.flink.api.connectors.source.SplitsAssignment;
import org.apache.flink.api.connectors.source.event.AddSplitEvent;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.state.CheckpointListener;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * A context class for the {@link SourceCoordinator}. Compared with {@link SplitEnumeratorContext} this class
 * allows interaction with state and sending {@link OperatorEvent} to the SourceOperator while
 * {@link SplitEnumeratorContext} only allows sending {@link SourceEvent}.
 *
 * @param <SplitT> the type of the splits.
 */
@Internal
public class SourceCoordinatorContext<SplitT extends SourceSplit> implements SplitEnumeratorContext<SplitT> {
	private final ExecutorNotifier notifier;
	private final ExecutionVertex[] subtasks;
	private final Map<Integer, ReaderInfo> registeredReaders;
	private final SplitAssignmentTracker<SplitT> assignmentTracker;

	public SourceCoordinatorContext(ExecutorNotifier notifier,
									ExecutionVertex[] subtasks) {
		this.notifier = notifier;
		this.subtasks = subtasks;
		this.registeredReaders = new HashMap<>();
		this.assignmentTracker = new SplitAssignmentTracker<>();
	}

	@Override
	public MetricGroup metricGroup() {
		return null;
	}

	@Override
	public CompletableFuture<Void> sendEventToSourceReader(int subtaskId, SourceEvent event) {
		return subtasks[subtaskId].handleOperatorEventFromCoordinator(event);
	}

	@Override
	public int numSubtasks() {
		return subtasks.length;
	}

	@Override
	public Map<Integer, ReaderInfo> registeredReaders() {
		return Collections.unmodifiableMap(registeredReaders);
	}

	@Override
	public Map<Integer, List<SplitT>> currentAssignment() {
		return assignmentTracker.currentSplitsAssignment();
	}

	@Override
	public void assignSplits(SplitsAssignment<SplitT> assignment) {
		if (assignment.type() == SplitsAssignment.Type.OVERRIDING) {
			throw new UnsupportedOperationException("The OVERRIDING assignment type is not " +
													"supported yet.");
		}
		assignmentTracker.recordSplitAssignment(assignment);
		assignment.assignment().forEach(
				(id, splits) -> sendEventToSourceOperator(id, new AddSplitEvent<>(splits))
		);
	}

	@Override
	public <T> void assignSplitAsync(Callable<T> callable,
									 BiConsumer<T, Throwable> handler,
									 long initialDelay,
									 long period) {
		notifier.notifyReadyAsync(callable, handler, initialDelay, period);
	}

	@Override
	public <T> void assignSplitAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
		notifier.notifyReadyAsync(callable, handler);
	}

	// --------- Package private additional methods for the DefaultSourceCoordinator ------------
	/**
	 * Access the state for the enumerator state.
	 */
	<T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		return null;
	}

	/**
	 * Send a source event to a source operator. The source operator is identified by its subtask id.
	 * This method is different from {@link #sendEventToSourceReader(int, SourceEvent)} that the
	 * latter is used by the {@link org.apache.flink.api.connectors.source.SplitEnumerator} to
	 * send {@link SourceEvent} to the {@link org.apache.flink.api.connectors.source.SourceReader}.
	 *
	 * @param subtaskId the subtask id of the source operator to send this event to.
	 * @param event the source event to send.
	 * @return a completable future which tells the result of the sending.
	 */
	CompletableFuture<Boolean> sendEventToSourceOperator(int subtaskId, OperatorEvent event) {
		return null;
	}

	/**
	 * Take a snapshot of this SourceCoordinatorContext and return the currently uncheckpointed splits
	 * assignment.
	 *
	 * @param checkpointId The id of the ongoing checkpoint.
	 * @param splitSerializer The serializer of the splits.
	 * @param out An ObjectOutput that can be used to
	 */
	void snapshotState(long checkpointId,
					   SimpleVersionedSerializer<SplitT> splitSerializer,
					   ObjectOutput out) throws Exception {
		out.writeObject(registeredReaders);
		assignmentTracker.snapshotState(checkpointId, splitSerializer, out);
	}

	/**
	 * Register a source reader.
	 *
	 * @param subtaskId the subtask id of the source reader.
	 * @param readerInfo the reader information of the source reader.
	 */
	void registerSourceReader(int subtaskId, ReaderInfo readerInfo) {
		registeredReaders.put(subtaskId, readerInfo);
	}

	/**
	 * Get the split to put back. This only happens when a source reader subtask has failed.
	 *
	 * @param failedSubtaskId the failed subtask id.
	 * @return A list of splits that needs to be added back to the
	 *         {@link org.apache.flink.api.connectors.source.SplitEnumerator}.
	 */
	List<SplitT> getAndRemoveUncheckpointedAssignment(int failedSubtaskId) {
		return assignmentTracker.getAndRemoveUncheckpointedAssignment(failedSubtaskId);
	}

	void restoreState(SimpleVersionedSerializer<SplitT> splitSerializer,
					  ObjectInput in) throws Exception {
		Map<Integer, ReaderInfo> readers = (Map<Integer, ReaderInfo>) in.readObject();
		registeredReaders.putAll(readers);
		assignmentTracker.restoreState(splitSerializer, in);
	}

	void onCheckpointComplete(long checkpointId) {
		assignmentTracker.onCheckpointComplete(checkpointId);
	}
}

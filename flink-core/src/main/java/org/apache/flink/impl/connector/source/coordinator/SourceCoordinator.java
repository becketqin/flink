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

package org.apache.flink.impl.connector.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.connectors.source.ReaderInfo;
import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.ReaderFailedEvent;
import org.apache.flink.api.connectors.source.event.ReaderRegistrationEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * A class that runs a {@link org.apache.flink.api.connectors.source.SplitEnumerator}.
 * It is responsible for the following:
 * 1. handle the operator events sent by the SourceOperator. This will also trigger a split assignment.
 * 2. collects the state of the SplitEnumerator and SourceCoordinatorContext to do checkpoint.
 *
 * <p>The first split assignment query is always triggered by a SourceReader registration.
 * After that the split assignment query can be triggered by one of the following:
 * 1. receiving a new SourceEvent from the SourceReader
 * 2. some splits are added back to the enumerator, probably due to a source reader failure.
 * 3. a new split is discovered
 */
@Internal
public class SourceCoordinator<SplitT extends SourceSplit, CheckpointT> implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinator.class);
	private final ExecutorService coordinatorExecutor;
	private final SplitEnumerator<SplitT, CheckpointT> enumerator;
	private final SimpleVersionedSerializer<CoordinatorState<SplitT, CheckpointT>> stateSerializer;
	private final ValueState<byte[]> coordinatorState;
	private final SourceCoordinatorContext<SplitT> context;

	public SourceCoordinator(ExecutorService coordinatorExecutor,
							 SplitEnumerator<SplitT, CheckpointT> enumerator,
							 SourceCoordinatorContext<SplitT> context,
							 SimpleVersionedSerializer<CoordinatorState<SplitT, CheckpointT>> stateSerializer) {
		this.coordinatorExecutor = coordinatorExecutor;
		this.enumerator = enumerator;
		this.coordinatorState = context.getState(new ValueStateDescriptor<>("CoordinatorState", byte[].class));
		this.stateSerializer = stateSerializer;
		this.context = context;
		this.enumerator.setSplitEnumeratorContext(context);
		this.enumerator.start();
	}

	@Override
	public void close() throws Exception {
		enumerator.close();
		coordinatorExecutor.shutdown();
	}

	/**
	 * Take a snapshot of the source coordinator. The invocation returns a future that will be completed
	 * with a null if the snapshot was taken successfully. Otherwise the future will be completed with
	 * an exception.
	 *
	 * @param checkpointId The checkpoint id of the snapshot being taken.
	 * @return A future that will be completed with null if the snapshot is successfully taken, or be
	 * completed with an exception otherwise.
	 */
	public Future<Exception> snapshotState(long checkpointId) {
		return coordinatorExecutor.submit(asCallable(() -> {
			try {
				CoordinatorState<SplitT, CheckpointT> coordState =
						new CoordinatorState<>(checkpointId, enumerator, context);
				coordinatorState.update(stateSerializer.serialize(coordState));
			} catch (IOException e) {
				LOG.warn("Failed to take snapshot on the SourceCoordinator.");
			}
		}));
	}

	/**
	 * Handles the operator event sent from the source operator of the given subtask id.
	 *
	 * @param subtaskId the subtask id of the operator event sender.
	 * @param event the received operator event.
	 */
	public Future<Exception> handleOperatorEvent(int subtaskId, OperatorEvent event) {
		return coordinatorExecutor.submit(asCallable(() -> {
			if (event instanceof SourceEvent) {
				enumerator.handleSourceEvent(subtaskId, (SourceEvent) event);
			} else if (event instanceof ReaderRegistrationEvent) {
				handleReaderRegistrationEvent((ReaderRegistrationEvent) event);
			} else if (event instanceof ReaderFailedEvent) {
				handleReaderFailedEvent((ReaderFailedEvent) event);
			}
			enumerator.updateAssignment();
		}));
	}

	// --------------------- private methods -------------
	private void handleReaderRegistrationEvent(ReaderRegistrationEvent event) {
		context.registerSourceReader(event.subtaskId(), new ReaderInfo(event.subtaskId(), event.location()));
	}

	private void handleReaderFailedEvent(ReaderFailedEvent event) {
		List<SplitT> splitsToAddBack = context.getAndRemoveUncheckpointedAssignment(event.subtaskId());
		enumerator.addSplitsBack(splitsToAddBack);
	}

	private static Callable<Exception> asCallable(Runnable runnable) {
		return () -> {
			try {
				runnable.run();
				return null;
			} catch (Exception e) {
				return e;
			}
		};
	}
}

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
import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.ReaderFailedEvent;
import org.apache.flink.api.connectors.source.event.ReaderRegistrationEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

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
public class DefaultSourceCoordinator<SplitT extends SourceSplit, CheckpointT> implements SourceCoordinator {
	private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinator.class);
	private final ExecutorService coordinatorExecutor;
	private final SplitEnumerator<SplitT, CheckpointT> enumerator;
	private final SimpleVersionedSerializer<CheckpointT> enumCheckpointSerializer;
	private final SimpleVersionedSerializer<SplitT> splitSerializer;
	private final ValueState<byte[]> coordinatorState;
	private final SourceCoordinatorContext<SplitT> context;

	public DefaultSourceCoordinator(ExecutorService coordinatorExecutor,
									SplitEnumerator<SplitT, CheckpointT> enumerator,
									SourceCoordinatorContext<SplitT> context,
									SimpleVersionedSerializer<SplitT> splitSerializer,
									SimpleVersionedSerializer<CheckpointT> enumCheckpointSerializer) {
		this.coordinatorExecutor = coordinatorExecutor;
		this.enumerator = enumerator;
		this.coordinatorState = context.getState(new ValueStateDescriptor<>("CoordinatorState", byte[].class));
		this.enumCheckpointSerializer = enumCheckpointSerializer;
		this.splitSerializer = splitSerializer;
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
	 * Initialize the state of this default source coordinator.
	 *
	 * @throws Exception when the state initialization failed.
	 */
	public void initializeState() throws Exception {
		byte[] bytes = coordinatorState.value();
		if (bytes != null) {
			fromBytes(bytes);
		}
	}

	/**
	 * Take a snapshot of the source coordinator. The invocation returns a future that will be completed
	 * with a null if the snapshot was taken successfully. Otherwise the future will be completed with
	 * an exception.
	 *
	 * @param checkpointId The checkpoint id of the snapshot being taken.
	 * @return A future that will be completed with null if the snapshot is successfully taken, or
	 * completed with an exception otherwise.
	 */
	@Override
	public CompletableFuture<Void> snapshotState(long checkpointId) {
		return CompletableFuture.runAsync(() -> {
			try {
				coordinatorState.update(toBytes(checkpointId));
			} catch (Exception e) {
				LOG.warn("Failed to take snapshot on the DefaultSourceCoordinator.");
			}
		}, coordinatorExecutor);
	}

	/**
	 * Handles the operator event sent from the source operator of the given subtask id.
	 *
	 * @param subtaskId the subtask id of the operator event sender.
	 * @param event the received operator event.
	 * @return A future that will be completed with null if the operator event  is successfully handled, or
	 * completed with an exception otherwise.
	 */
	@Override
	public CompletableFuture<Void> handleOperatorEvent(int subtaskId, OperatorEvent event) {
		return CompletableFuture.runAsync(() -> {
			if (event instanceof SourceEvent) {
				enumerator.handleSourceEvent(subtaskId, (SourceEvent) event);
			} else if (event instanceof ReaderRegistrationEvent) {
				handleReaderRegistrationEvent((ReaderRegistrationEvent) event);
			} else if (event instanceof ReaderFailedEvent) {
				handleReaderFailedEvent((ReaderFailedEvent) event);
			}
		}, coordinatorExecutor);
	}

	@Override
	public void onCheckpointComplete(long checkpointId) {
		context.onCheckpointComplete(checkpointId);
	}

	// --------------------- Serde -----------------------
	/**
	 * Serialize the coordinator state. The current implementation may not be super efficient,
	 * but it should not matter that much because most of the state should be rather small.
	 * Large states themselves may already be a problem regardless of how the serialization
	 * is implemented.
	 *
	 * @return A byte array containing the serialized state of the source coordinator.
	 * @throws Exception When something goes wrong in serialization.
	 */
	private byte[] toBytes(long checkpointId) throws Exception {
		CheckpointT enumCkpt = enumerator.snapshotState();

		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(baos)) {
			out.write(enumCheckpointSerializer.getVersion());
			out.write(enumCheckpointSerializer.serialize(enumCkpt));
			context.snapshotState(checkpointId, splitSerializer, out);
			out.flush();
			return baos.toByteArray();
		}
	}

	/**
	 * Restore the state of this source coordinator from the state bytes.
	 *
	 * @param bytes The checkpoint bytes that was returned from {@link #toBytes(long)}
	 * @throws Exception When the deserialization failed.
	 */
	private void fromBytes(byte[] bytes) throws Exception {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInput in = new ObjectInputStream(bais)) {
			int enumSerializerVersion = in.readInt();
			byte[] serializedEnumChkpt = (byte[]) in.readObject();
			CheckpointT enumChkpt = enumCheckpointSerializer.deserialize(enumSerializerVersion, serializedEnumChkpt);
			enumerator.restoreState(enumChkpt);
			context.restoreState(splitSerializer, in);
		}
	}

	// --------------------- private methods -------------
	private void handleReaderRegistrationEvent(ReaderRegistrationEvent event) {
		context.registerSourceReader(event.subtaskId(), new ReaderInfo(event.subtaskId(), event.location()));
		enumerator.addReader(event.subtaskId());
	}

	private void handleReaderFailedEvent(ReaderFailedEvent event) {
		List<SplitT> splitsToAddBack = context.getAndRemoveUncheckpointedAssignment(event.subtaskId());
		enumerator.addSplitsBack(splitsToAddBack, event.subtaskId());
	}
}

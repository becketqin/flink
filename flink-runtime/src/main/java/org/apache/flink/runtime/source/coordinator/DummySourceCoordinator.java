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

import org.apache.flink.api.connectors.source.event.OperatorEvent;
import org.apache.flink.api.connectors.source.event.SourceEvent;

import java.util.concurrent.CompletableFuture;

/**
 * A singleton dummy source coordinator class to handle the cases where a ExecutionJobVertex does not have
 * a source coordinator. This helps unify the logic in CheckpointCoordinator.
 */
public class DummySourceCoordinator implements SourceCoordinator {
	public static final DummySourceCoordinator INSTANCE = new DummySourceCoordinator();

	private DummySourceCoordinator() {};

	@Override
	public CompletableFuture<Void> snapshotState(long checkpointId) {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public CompletableFuture<Void> handleOperatorEvent(int subtaskId, OperatorEvent event) {
		if (event instanceof SourceEvent) {
			throw new UnsupportedOperationException("The DummySourceCoordinator should never receive source event.");
		}
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void close() throws Exception {
		// Do nothing.
	}

	@Override
	public void onCheckpointComplete(long checkpointId) {
		// Do nothing.
	}
}

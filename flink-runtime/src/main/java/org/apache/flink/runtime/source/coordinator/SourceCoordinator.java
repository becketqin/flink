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

import java.util.concurrent.CompletableFuture;

/**
 * The interface for source coordinator.
 */
public interface SourceCoordinator extends AutoCloseable {

	/**
	 * Take a snapshot of the source coordinator. The invocation returns a future that will be completed
	 * with a null if the snapshot was taken successfully. Otherwise the future will be completed with
	 * an exception.
	 *
	 * @param checkpointId The checkpoint id of the snapshot being taken.
	 * @return A future that will be completed with null if the snapshot is successfully taken, or
	 * completed with an exception otherwise.
	 */
	CompletableFuture<Void> snapshotState(long checkpointId);

	/**
	 * Handles the operator event sent from the source operator of the given subtask id.
	 *
	 * @param subtaskId the subtask id of the operator event sender.
	 * @param event the received operator event.
	 * @return A future that will be completed with null if the operator event  is successfully handled, or
	 * completed with an exception otherwise.
	 */
	CompletableFuture<Void> handleOperatorEvent(int subtaskId, OperatorEvent event);

	/**
	 * Notify the success of a completed checkpoint.
	 *
	 * @param checkpointId The ID of the successful checkpoint.
	 */
	void onCheckpointComplete(long checkpointId);
}

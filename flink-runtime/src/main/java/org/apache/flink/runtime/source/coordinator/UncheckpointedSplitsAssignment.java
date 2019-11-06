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

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.api.connectors.source.SplitsAssignment;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.function.FunctionWithException;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A class that helps track the uncheckpointed splits assignments per checkpoint.
 * This is needed when a SourceReader fails and we only want to restart the sub-graph
 * of that failed SourceReader, without restarting the entire DAG. In that case,
 * the state of the SplitEnumerator needs to be partially restored. More specifically,
 * the splits assigned to the failed SourceReader will have to be rolled back to
 * the last successful checkpoint.
 */
class UncheckpointedSplitsAssignment<SplitT extends SourceSplit> {
	// All the split assignments since the last successful checkpoint.
	private final SortedMap<Long, Map<Integer, List<SplitT>>> assignmentsByCheckpoints;
	// The split assignments since the last checkpoint attempt.
	private Map<Integer, List<SplitT>> uncheckpointedAssignments;

	UncheckpointedSplitsAssignment() {
		assignmentsByCheckpoints = new TreeMap<>();
		uncheckpointedAssignments = new HashMap<>();
	}

	void recordNewAssignment(SplitsAssignment<SplitT> assignment) {
		if (assignment.type() == SplitsAssignment.Type.INCREMENTAL) {
			assignment.assignment().forEach((id, splits) -> {
				uncheckpointedAssignments.computeIfAbsent(id, ignored -> new ArrayList<>())
										 .addAll(splits);
			});
		} else {
			throw new UnsupportedOperationException("The OVERRIDING assignment is not supported yet.");
		}
	}

	void snapshotState(long checkpointId,
					   SimpleVersionedSerializer<SplitT> splitSerializer,
					   ObjectOutput out) throws Exception {
		assignmentsByCheckpoints.put(checkpointId, uncheckpointedAssignments);
		uncheckpointedAssignments = new HashMap<>();
		Map<Long, Map<Integer, List<byte[]>>> serializedState = convertAssignmentsByCheckpoints(
				assignmentsByCheckpoints, splitSerializer::serialize);
		out.writeObject(serializedState);
	}

	void onCheckpointCompleted(long checkpointId) {
		assignmentsByCheckpoints.entrySet().removeIf(entry -> entry.getKey() <= checkpointId);
	}

	List<SplitT> splitsToAddBack(int subtaskId) {
		List<SplitT> splits = new ArrayList<>();
		assignmentsByCheckpoints.values().forEach(assignments -> {
			List<SplitT> splitsForSubtask = assignments.remove(subtaskId);
			if (splitsForSubtask != null) {
				splits.addAll(splitsForSubtask);
			}
		});
		return splits;
	}

	@SuppressWarnings("unchecked")
	void restoreState(SimpleVersionedSerializer<SplitT> splitSerializer,
					  int serializedVersion,
					  ObjectInput in) throws Exception {
		Map<Long, Map<Integer, List<byte[]>>> serializedState =
				(Map<Long, Map<Integer, List<byte[]>>>) in.readObject();
		Map<Long, Map<Integer, List<SplitT>>> deserializedState = convertAssignmentsByCheckpoints(
				serializedState,
				(byte[] splitBytes) -> splitSerializer.deserialize(serializedVersion, splitBytes));
		assignmentsByCheckpoints.putAll(deserializedState);
	}

	// ------ private helpers --------

	private static <S, T> Map<Long, Map<Integer, List<T>>> convertAssignmentsByCheckpoints(
			Map<Long, Map<Integer, List<S>>> uncheckpiontedAssignments,
			FunctionWithException<S, T, Exception> converter) throws Exception {
		// First Serialize splits into bytes.
		Map<Long, Map<Integer, List<T>>> targetSplitsContextCkpt = new HashMap<>(uncheckpiontedAssignments.size());
		for (Map.Entry<Long, Map<Integer, List<S>>> ckptEntry : uncheckpiontedAssignments.entrySet()) {
			targetSplitsContextCkpt.put(
					ckptEntry.getKey(),
					CoordinatorSerdeUtils.convertAssignment(ckptEntry.getValue(), converter));
		}
		return targetSplitsContextCkpt;
	}
}

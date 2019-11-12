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

import org.apache.flink.api.connectors.source.SplitsAssignment;
import org.apache.flink.impl.connector.source.mocks.MockSourceSplit;
import org.apache.flink.impl.connector.source.mocks.MockSourceSplitSerializer;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for @link {@link SplitAssignmentTracker}.
 */
public class SplitAssignmentTrackerTest {

	@Test
	public void testRecordIncrementalSplitAssignment() {
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
		tracker.recordSplitAssignment(getAssignments(3, 0, SplitsAssignment.Type.INCREMENTAL));
		tracker.recordSplitAssignment(getAssignments(2, 6, SplitsAssignment.Type.INCREMENTAL));

		verifyAssignment(0, Arrays.asList("0", "6"), tracker.currentSplitsAssignment());
		verifyAssignment(1, Arrays.asList("1", "2", "7", "8"), tracker.currentSplitsAssignment());
		verifyAssignment(2, Arrays.asList("3", "4", "5"), tracker.currentSplitsAssignment());
	}

	@Test
	public void testSnapshotAndRestore() throws Exception {
		SplitAssignmentTracker<MockSourceSplit> tracker = new SplitAssignmentTracker<>();
		tracker.recordSplitAssignment(getAssignments(3, 0, SplitsAssignment.Type.INCREMENTAL));

		byte[] bytes;
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 ObjectOutput out = new ObjectOutputStream(baos)) {
			tracker.snapshotState(123L, new MockSourceSplitSerializer(), out);
			out.flush();
			bytes = baos.toByteArray();
		}

		SplitAssignmentTracker<MockSourceSplit> deserializedTracker;
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			 ObjectInput in = new ObjectInputStream(bais)) {
			deserializedTracker = new SplitAssignmentTracker<>();
			deserializedTracker.restoreState(new MockSourceSplitSerializer(), in);
		}

		assertEquals(deserializedTracker.currentSplitsAssignment(), tracker.currentSplitsAssignment());
	}

	/**
	 * Create a SplitsAssignment. The assignments looks like following:
	 * Subtask 0: Splits {0}
	 * Subtask 1: Splits {1, 2}
	 * Subtask 2: Splits {3, 4, 5}
	 */
	private SplitsAssignment<MockSourceSplit> getAssignments(int numSubtasks,
															 int startingSplitId,
															 SplitsAssignment.Type type) {
		Map<Integer, List<MockSourceSplit>> assignments = new HashMap<>();
		int splitId = startingSplitId;
		for (int subtaskIndex = 0; subtaskIndex < numSubtasks; subtaskIndex++) {
			List<MockSourceSplit> subtaskAssignment = new ArrayList<>();
			for (int j = 0; j < subtaskIndex + 1; j++) {
				subtaskAssignment.add(new MockSourceSplit(splitId++));
			}
			assignments.put(subtaskIndex, subtaskAssignment);
		}
		return new SplitsAssignment<>(assignments, type);
	}

	private void verifyAssignment(int subtaskIndex,
								  List<String> expectedSplitIds,
								  Map<Integer, List<MockSourceSplit>> assignment) {
		List<MockSourceSplit> taskAssignment = assignment.get(subtaskIndex);
		assertEquals(taskAssignment.size(), expectedSplitIds.size());
		for (int i = 0; i < taskAssignment.size(); i++) {
			assertEquals(taskAssignment.get(i).splitId(), expectedSplitIds.get(i));
		}
	}
}

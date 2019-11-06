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

import org.apache.flink.util.function.FunctionWithException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A package private util class containing helper methods for state serde for coordinator.
 */
class CoordinatorSerdeUtils {

	private CoordinatorSerdeUtils() {}

	static <S, T> Map<Integer, List<T>> convertAssignment(
			Map<Integer, List<S>> assignment,
			FunctionWithException<S, T, Exception> converter) throws Exception {
		Map<Integer, List<T>> convertedAssignments = new HashMap<>();
		for (Map.Entry<Integer, List<S>> assignmentEntry : assignment.entrySet()) {
			List<T> serializedSplits = convertedAssignments.compute(
					assignmentEntry.getKey(),
					(taskId, ignored) -> new ArrayList<>(assignmentEntry.getValue().size()));
			for (S source : assignmentEntry.getValue()) {
				serializedSplits.add(converter.apply(source));
			}
		}
		return convertedAssignments;
	}
}

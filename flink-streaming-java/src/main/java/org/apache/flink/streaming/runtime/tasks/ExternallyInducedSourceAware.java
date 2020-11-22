/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.connector.source.ExternallyInducedSourceReader;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.streaming.api.operators.SourceOperator;

import java.util.List;
import java.util.function.Consumer;

/**
 *  A decorative interface for {@link StreamTask} that works with
 *  {@link ExternallyInducedSourceReader}.
 */
public interface ExternallyInducedSourceAware {

	/**
	 * Get a checkpoint trigger hook that can be invoked when the corresponding
	 * ExternallyInducedSource is ready to checkpoint.
	 *
	 * @param sourceOperator the SourceOperator containing the ExternallyInducedSource.
	 * @param inputChannelInfos the information associated with the SourceOperator.
	 * @return A checkpoint triggering hook that will be invoked when an
	 * ExternallyInducedSourceReader finishes its checkpoint.
	 */
	Consumer<Long> getCheckpointTriggeringHook(
		SourceOperator<?, ?> sourceOperator,
		List<InputChannelInfo> inputChannelInfos);
}

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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.connectors.source.Source;
import org.apache.flink.runtime.operators.util.TaskConfig;

import java.util.List;

/**
 * A JobVertex containing a {@link Source}.
 */
public class SourceVertex extends JobVertex {

	public SourceVertex(String name,
						JobVertexID primaryId,
						List<JobVertexID> alternativeIds,
						List<OperatorID> operatorIds,
						List<OperatorID> alternativeOperatorIds) {
		super(name, primaryId, alternativeIds, operatorIds, alternativeOperatorIds);
	}

	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		super.initializeOnMaster(loader);
		Source source = loadSource(loader);
		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			setSplitEnumerator(source.createEnumerator(getConfiguration()));
			setSplitSerializer(source.getSplitSerializer());
			setSplitEnumeratorCheckpointSerializer(source.getEnumeratorCheckpointSerializer());
		} finally {
			// restore original classloader
			Thread.currentThread().setContextClassLoader(original);
		}
	}

	private Source loadSource(ClassLoader loader) {
		final UserCodeWrapper<Source> wrapper;

		try {
			wrapper = new TaskConfig(getConfiguration()).getStubWrapper(loader);
		} catch (Throwable t) {
			throw new RuntimeException("Deserializing the Source failed: " + t.getMessage(), t);
		}

		if (wrapper == null) {
			throw new RuntimeException("No Source present in task configuration.");
		}

		try {
			return wrapper.getUserCodeObject(Source.class, loader);
		} catch (Throwable t) {
			throw new RuntimeException("Instantiating the Source failed: " + t.getMessage(), t);
		}
	}
}

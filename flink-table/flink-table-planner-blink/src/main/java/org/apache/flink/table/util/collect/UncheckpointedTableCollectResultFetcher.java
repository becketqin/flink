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

package org.apache.flink.table.util.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * An {@link AbstractTableCollectResultFetcher} which ignores checkpoints and provides the results as fast as possible.
 * Note that this fetcher will fail when the sink restarts and it does not guarantee exactly-once semantics.
 *
 * @param <T> result type
 */
public class UncheckpointedTableCollectResultFetcher<T> extends AbstractTableCollectResultFetcher<T> {

	private static final String INIT_VERSION = "";

	private static final Logger LOG = LoggerFactory.getLogger(UncheckpointedTableCollectResultFetcher.class);

	private String version;
	private long token;
	private boolean terminated;

	public UncheckpointedTableCollectResultFetcher(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<T> serializer,
			String finalResultListAccumulatorName,
			String finalResultTokenAccumulatorName) {
		super(operatorIdFuture, serializer, finalResultListAccumulatorName, finalResultTokenAccumulatorName);
		this.version = INIT_VERSION;
		this.token = 0;
		this.terminated = false;
	}

	@Override
	public List<T> nextBatch() {
		if (terminated) {
			return Collections.emptyList();
		}

		while (true) {
			if (isJobTerminated()) {
				// job terminated, read results from accumulator
				terminated = true;
				return getAccumulatorResults(token);
			}

			CollectCoordinationResponse.DeserializedResponse<T> deserializedResponse;
			try {
				deserializedResponse = sendRequest(version, token);
			} catch (InterruptedException | ExecutionException e) {
				LOG.warn("An exception occurs when fetching query results", e);
				sleepBeforeRetry();
				continue;
			}

			String responseVersion = deserializedResponse.getVersion();
			long responseToken = deserializedResponse.getToken();
			List<T> results = deserializedResponse.getResults();

			if (INIT_VERSION.equals(version)) {
				// first response, update version accordingly
				version = responseVersion;
			} else if (!version.equals(responseVersion)) {
				throw new RuntimeException("Table collect sink has restarted");
			}

			if (!results.isEmpty()) {
				Preconditions.checkArgument(
					token == responseToken,
					"Response token does not equal to the expected token. This is a bug.");
				token += results.size();
				return results;
			}

			sleepBeforeRetry();
		}
	}
}

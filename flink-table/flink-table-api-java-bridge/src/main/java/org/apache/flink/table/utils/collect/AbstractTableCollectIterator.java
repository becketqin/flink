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

package org.apache.flink.table.utils.collect;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequester;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Common implementation of {@link TableCollectIterator} for both batch and streaming jobs.
 *
 * @param <T> result type of the job
 */
public abstract class AbstractTableCollectIterator<T> implements TableCollectIterator {

	private static final String INIT_VERSION = "";
	private static final int RETRY_INTERVAL_MILLIS = 100;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractTableCollectIterator.class);

	private final CompletableFuture<OperatorID> operatorIdFuture;
	private final TypeSerializer<T> serializer;
	private final String finalResultAccumulatorName;
	private JobClient jobClient;

	protected final LinkedList<Tuple2<Boolean, Row>> bufferedResults;

	private String version;
	private long token;
	private boolean terminated;

	public AbstractTableCollectIterator(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<T> serializer,
			String finalResultAccumulatorName) {
		this.operatorIdFuture = operatorIdFuture;
		this.serializer = serializer;
		this.finalResultAccumulatorName = finalResultAccumulatorName;

		this.bufferedResults = new LinkedList<>();

		this.version = INIT_VERSION;
		this.token = 0;
		this.terminated = false;
	}

	@Override
	public boolean hasNext() {
		// we have to make sure that the next result exists
		// it is possible that there is no more result but the job is still running
		if (bufferedResults.isEmpty()) {
			fetchMoreResults();
		}
		return !bufferedResults.isEmpty();
	}

	@Override
	public Tuple2<Boolean, Row> next() {
		if (bufferedResults.isEmpty()) {
			fetchMoreResults();
		}
		if (bufferedResults.isEmpty()) {
			return null;
		} else {
			return bufferedResults.removeFirst();
		}
	}

	@Override
	public TableCollectIterator configure(JobClient jobClient) {
		this.jobClient = jobClient;
		return this;
	}

	abstract protected void fetchMoreResults();

	protected List<T> fetchResultsFromCoordinator() {
		Preconditions.checkNotNull(jobClient, "Job client must be configured before first use.");
		Preconditions.checkArgument(
			jobClient instanceof CoordinationRequester,
			"Job client must be a CoordinationRequester. This is a bug.");
		CoordinationRequester requester = (CoordinationRequester) jobClient;

		OperatorID operatorId = operatorIdFuture.getNow(null);
		Preconditions.checkNotNull(operatorId, "Unknown operator ID. This is a bug.");

		if (terminated) {
			return Collections.emptyList();
		}

		while (true) {
			try {
				JobStatus status = jobClient.getJobStatus().get();
				if (status.isGloballyTerminalState()) {
					// job terminated, read results from accumulator
					terminated = true;
					return getAccumulatorResults();
				}

				CollectCoordinationRequest request = new CollectCoordinationRequest(version, token);
				CollectCoordinationResponse response =
					(CollectCoordinationResponse) requester.sendCoordinationRequest(operatorId, request).get();
				CollectCoordinationResponse.DeserializedResponse<T> deserializedResponse =
					response.getDeserializedResponse(serializer);

				String responseVersion = deserializedResponse.getVersion();
				List<T> results = deserializedResponse.getResults();

				if (INIT_VERSION.equals(version)) {
					// first response, update version accordingly
					version = responseVersion;
				} else if (!version.equals(responseVersion)) {
					// TODO support at least once / exactly once
					throw new RuntimeException("Table collect sink has restarted");
				}

				if (!results.isEmpty()) {
					// TODO deal with incompatible tokens
					token += results.size();
					return results;
				}
			} catch (Exception e) {
				// TODO
				//  1. deal with exceptions, currently we just retry
				//  one known issue: it is very likely that a JobNotFound exception is thrown
				//  even if the status is still RUNNING
				//  2. job failures might be shaded by iterator
				LOG.warn("An exception occurs when fetching query results", e);
			}

			try {
				// TODO a more proper retry strategy?
				Thread.sleep(RETRY_INTERVAL_MILLIS);
			} catch (InterruptedException e) {
				LOG.warn("Interrupted when sleeping before a retry", e);
			}
		}
	}

	private List<T> getAccumulatorResults() {
		try {
			JobExecutionResult executionResult = jobClient.getJobExecutionResult(getClass().getClassLoader()).get();
			ArrayList<byte[]> serializedResults = executionResult.getAccumulatorResult(finalResultAccumulatorName);
			return SerializedListAccumulator.deserializeList(serializedResults, serializer);
		} catch (Exception e) {
			LOG.warn("Failed to fetch final results in accumulators, some results might be lost", e);
			return Collections.emptyList();
		}
	}
}

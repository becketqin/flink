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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequester;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Common implementation of a {@link TableCollectResultFetcher}.
 *
 * @param <T> result type
 */
public abstract class AbstractTableCollectResultFetcher<T> implements TableCollectResultFetcher<T> {

	private static final int RETRY_INTERVAL_MILLIS = 100;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractTableCollectResultFetcher.class);

	private final CompletableFuture<OperatorID> operatorIdFuture;
	protected final TypeSerializer<T> serializer;
	private final String finalResultListAccumulatorName;
	private final String finalResultTokenAccumulatorName;

	protected JobClient jobClient;

	public AbstractTableCollectResultFetcher(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<T> serializer,
			String finalResultListAccumulatorName,
			String finalResultTokenAccumulatorName) {
		this.operatorIdFuture = operatorIdFuture;
		this.serializer = serializer;
		this.finalResultListAccumulatorName = finalResultListAccumulatorName;
		this.finalResultTokenAccumulatorName = finalResultTokenAccumulatorName;
	}

	@Override
	public void configure(JobClient jobClient) {
		Preconditions.checkArgument(
			jobClient instanceof CoordinationRequester,
			"Job client must be a CoordinationRequester. This is a bug.");
		this.jobClient = jobClient;
	}

	protected CollectCoordinationResponse.DeserializedResponse<T> sendRequest(
			String version,
			long token) throws InterruptedException, ExecutionException {
		Preconditions.checkNotNull(jobClient, "Job client must be configured before first use.");
		CoordinationRequester requester = (CoordinationRequester) jobClient;

		OperatorID operatorId = operatorIdFuture.getNow(null);
		Preconditions.checkNotNull(operatorId, "Unknown operator ID. This is a bug.");

		CollectCoordinationRequest request = new CollectCoordinationRequest(version, token);
		CollectCoordinationResponse response =
			(CollectCoordinationResponse) requester.sendCoordinationRequest(operatorId, request).get();
		return response.getDeserializedResponse(serializer);
	}

	protected List<T> getAccumulatorResults(long token) {
		JobExecutionResult executionResult;
		try {
			executionResult = jobClient.getJobExecutionResult(getClass().getClassLoader()).get();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException("Failed to fetch job execution result", e);
		}

		ArrayList<byte[]> serializedList = executionResult.getAccumulatorResult(finalResultListAccumulatorName);
		Long begin = executionResult.getAccumulatorResult(finalResultTokenAccumulatorName);
		if (serializedList == null || begin == null) {
			// job terminates abnormally
			return Collections.emptyList();
		}

		ArrayList<byte[]> subList = new ArrayList<>();
		for (int i = (int) (token - begin); i < serializedList.size(); i++) {
			subList.add(serializedList.get(i));
		}
		try {
			return SerializedListAccumulator.deserializeList(subList, serializer);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Failed to deserialize results from accumulator", e);
		}
	}

	protected boolean isJobTerminated() {
		try {
			JobStatus status = jobClient.getJobStatus().get();
			return status.isGloballyTerminalState();
		} catch (Exception e) {
			// TODO
			//  This is sort of hack.
			//  Currently different execution environment will have different behaviors
			//  when fetching a finished job status.
			//  For example, standalone session cluster will return a normal FINISHED,
			//  while mini cluster will throw IllegalStateException,
			//  and yarn per job will throw ApplicationNotFoundException.
			//  We have to assume that job has finished in this case.
			//  Change this when these behaviors are unified.
			LOG.warn("Failed to get job status so we assume that the job has terminated. Some data might be lost.", e);
			return true;
		}
	}

	protected void sleepBeforeRetry() {
		try {
			// TODO a more proper retry strategy?
			Thread.sleep(RETRY_INTERVAL_MILLIS);
		} catch (InterruptedException e) {
			LOG.warn("Interrupted when sleeping before a retry", e);
		}
	}
}

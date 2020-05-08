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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.table.utils.collect.TableCollectIterator;
import org.apache.flink.types.Row;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

/**
 * Implementation of {@link TableCollectIterator}.
 *
 * @param <T> result type
 */
public class TableCollectIteratorImpl<T> implements TableCollectIterator {

	private final TableCollectResultHandler<T> handler;
	private final TableCollectResultFetcher<T> fetcher;

	private final LinkedList<T> bufferedResults;

	public TableCollectIteratorImpl(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<T> serializer,
			String finalResultListAccumulatorName,
			String finalResultTokenAccumulatorName,
			TableCollectResultHandler<T> handler,
			boolean checkpointed) {
		this.handler = handler;
		if (checkpointed) {
			this.fetcher = new CheckpointedTableCollectResultFetcher<>(
				operatorIdFuture, serializer, finalResultListAccumulatorName, finalResultTokenAccumulatorName);
		} else {
			this.fetcher = new UncheckpointedTableCollectResultFetcher<>(
				operatorIdFuture, serializer, finalResultListAccumulatorName, finalResultTokenAccumulatorName);
		}

		this.bufferedResults = new LinkedList<>();
	}

	@Override
	public boolean hasNext() {
		// we have to make sure that the next result exists
		// it is possible that there is no more result but the job is still running
		if (bufferedResults.isEmpty()) {
			bufferedResults.addAll(fetcher.nextBatch());
		}
		return !bufferedResults.isEmpty();
	}

	@Override
	public Tuple2<Boolean, Row> next() {
		if (bufferedResults.isEmpty()) {
			bufferedResults.addAll(fetcher.nextBatch());
		}
		if (bufferedResults.isEmpty()) {
			return null;
		} else {
			return handler.handle(bufferedResults.removeFirst());
		}
	}

	@Override
	public TableCollectIterator configure(JobClient jobClient) {
		fetcher.configure(jobClient);
		return this;
	}
}

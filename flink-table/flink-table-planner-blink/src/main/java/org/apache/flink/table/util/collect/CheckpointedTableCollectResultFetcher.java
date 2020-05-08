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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.runtime.util.ResettableExternalBuffer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * An {@link AbstractTableCollectResultFetcher} which provides exactly-once semantics.
 *
 * @param <T> result type
 */
public class CheckpointedTableCollectResultFetcher<T> extends AbstractTableCollectResultFetcher<T> {

	private static final String INIT_VERSION = "";
	private static final long MEMORY_SIZE = 256 * MemoryManager.DEFAULT_PAGE_SIZE;

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointedTableCollectResultFetcher.class);

	private final BinaryRow reuseRow;
	private final BinaryWriter reuseWriter;

	private final MemoryManager memoryManager;
	private final IOManager ioManager;
	private final int pageNum;

	// TODO we currently don't have an external buffer which can store any type,
	//  so we have to temporarily use ResettableExternalBuffer,
	//  refactoring ResettableExternalBuffer instead of squeezing into it might be better
	private ResettableExternalBuffer uncheckpointedBuffer;
	private ResettableExternalBuffer checkpointedBuffer;
	private ResettableExternalBuffer.BufferIterator checkpointedIterator;

	private String version;
	private long token;
	private long checkpointedToken;
	private long lastCheckpointId;

	private boolean terminated;
	private boolean closed;

	public CheckpointedTableCollectResultFetcher(
			CompletableFuture<OperatorID> operatorIdFuture,
			TypeSerializer<T> serializer,
			String finalResultListAccumulatorName,
			String finalResultTokenAccumulatorName) {
		super(operatorIdFuture, serializer, finalResultListAccumulatorName, finalResultTokenAccumulatorName);

		this.reuseRow = new BinaryRow(1);
		this.reuseWriter = new BinaryRowWriter(reuseRow);

		Map<MemoryType, Long> memoryPools = new EnumMap<>(MemoryType.class);
		memoryPools.put(MemoryType.HEAP, MEMORY_SIZE);
		this.memoryManager = new MemoryManager(memoryPools, MemoryManager.DEFAULT_PAGE_SIZE);
		this.ioManager = new IOManagerAsync();
		this.pageNum = (int) (MEMORY_SIZE / memoryManager.getPageSize() / 2);

		this.uncheckpointedBuffer = newBuffer();
		this.checkpointedBuffer = newBuffer();

		this.version = INIT_VERSION;
		this.token = 0;
		this.checkpointedToken = 0;
		this.lastCheckpointId = Long.MIN_VALUE;

		this.terminated = false;
	}

	@Override
	public List<T> nextBatch() {
		if (closed) {
			return Collections.emptyList();
		}

		List<T> checkpointedResults = getNextCheckpointedResult();
		if (checkpointedResults.size() > 0) {
			// we still have checkpointed results, just use them
			return checkpointedResults;
		} else if (terminated) {
			// no results, but job has terminated, we have to return
			return checkpointedResults;
		}

		// we're going to fetch some more
		while (true) {
			if (isJobTerminated()) {
				// job terminated, read results from accumulator
				// and move all results from uncheckpointed buffer to checkpointed buffer
				terminated = true;
				addToUncheckpointedBuffer(getAccumulatorResults(token));
				checkpointResults();
			} else {
				// job still running, try to fetch some results
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
				long responseCheckpointId = deserializedResponse.getLastCheckpointId();
				List<T> results = deserializedResponse.getResults();

				if (INIT_VERSION.equals(version)) {
					// first response, update version accordingly
					version = responseVersion;
					token = responseToken;
					checkpointedToken = token;
					lastCheckpointId = responseCheckpointId;
				} else if (!version.equals(responseVersion)) {
					// sink restarted

					// NOTE: if this condition fails,
					// that means client does not know that a new checkpoint happens,
					// which means that data after this checkpoint has not affected the client,
					// so client just work as normal
					if (responseCheckpointId <= lastCheckpointId) {
						// uncheckpointed data has influenced the client, clear uncheckpointed data
						uncheckpointedBuffer.reset();
						token = checkpointedToken;
					}
					version = responseVersion;
				} else {
					// version matched
					if (lastCheckpointId < responseCheckpointId) {
						// a new checkpoint happens
						checkpointResults();
						lastCheckpointId = responseCheckpointId;
					}

					if (!results.isEmpty()) {
						Preconditions.checkArgument(
							token == responseToken,
							"Response token does not equal to the expected token. This is a bug.");
						token += results.size();
						addToUncheckpointedBuffer(results);
					}
				}
			}

			// try to return results after fetching
			checkpointedResults = getNextCheckpointedResult();
			if (checkpointedResults.size() > 0) {
				// ok, we have results this time
				return checkpointedResults;
			} else if (terminated) {
				// still no results, but job has terminated, we have to return
				return checkpointedResults;
			} else {
				// still no results, but job is still running, retry
				sleepBeforeRetry();
			}
		}
	}

	private ResettableExternalBuffer newBuffer() {
		return new ResettableExternalBuffer(
			ioManager,
			new LazyMemorySegmentPool(this, memoryManager, pageNum),
			new BinaryRowSerializer(1),
			// we're not using newBuffer(beginRow) so this is OK
			false);
	}

	private void addToUncheckpointedBuffer(List<T> results) {
		for (T result : results) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
			try {
				serializer.serialize(result, wrapper);
				reuseWriter.writeBinary(0, baos.toByteArray());
				reuseWriter.complete();
				uncheckpointedBuffer.add(reuseRow);
				reuseWriter.reset();
			} catch (IOException e) {
				// this shouldn't be possible
				LOG.warn(
					"An error occurs when serializing from ResettableExternalBuffer. Some data might be lost.", e);
			}
		}
	}

	private void checkpointResults() {
		ResettableExternalBuffer oldCheckpointedBuffer = checkpointedBuffer;

		// close last checkpointed results first
		if (checkpointedIterator != null) {
			checkpointedIterator.close();
		}
		checkpointedBuffer.reset();

		uncheckpointedBuffer.complete();
		checkpointedBuffer = uncheckpointedBuffer;
		checkpointedIterator = checkpointedBuffer.newIterator();

		uncheckpointedBuffer = oldCheckpointedBuffer;

		checkpointedToken = token;
	}

	private List<T> getNextCheckpointedResult() {
		List<T> ret;

		if (checkpointedIterator == null) {
			// checkpointed results hasn't been initialized,
			// which means that no more checkpointed results have occurred
			ret = Collections.emptyList();
		} else if (checkpointedIterator.advanceNext()) {
			BinaryRow binaryRow = checkpointedIterator.getRow();
			byte[] bytes = binaryRow.getBinary(0);
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(bais);
			try {
				T result = serializer.deserialize(wrapper);
				ret = Collections.singletonList(result);
			} catch (IOException e) {
				// this shouldn't be possible
				LOG.warn(
					"An error occurs when deserializing from ResettableExternalBuffer. Some data might be lost.", e);
				ret = Collections.emptyList();
			}
		} else {
			ret = Collections.emptyList();
		}

		if (terminated && ret.isEmpty()) {
			// no more results, close buffer and memory manager and we're done
			close();
		}

		return ret;
	}

	private void close() {
		if (closed) {
			return;
		}

		try {
			if (checkpointedIterator != null) {
				checkpointedIterator.close();
				checkpointedIterator = null;
			}
			uncheckpointedBuffer.close();
			checkpointedBuffer.close();

			memoryManager.shutdown();
			ioManager.close();
		} catch (Exception e) {
			LOG.warn("Error when closing ResettableExternalBuffers", e);
		}
		closed = true;
	}
}

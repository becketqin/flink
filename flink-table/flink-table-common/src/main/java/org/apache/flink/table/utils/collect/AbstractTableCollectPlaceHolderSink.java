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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * A place holder sink for retrieving query results to the client.
 * It will be changed into dedicated sink operators in planners.
 */
@Internal
public abstract class AbstractTableCollectPlaceHolderSink<T> implements TableSink<T> {

	protected final TableSchema schema;
	private final int maxResultsPerBatch;
	private final boolean checkpointed;

	private TableCollectIterator iterator;

	public AbstractTableCollectPlaceHolderSink(
			TableSchema schema,
			int maxResultsPerBatch,
			boolean checkpointed) {
		this.schema = schema;
		this.maxResultsPerBatch = maxResultsPerBatch;
		this.checkpointed = checkpointed;
	}

	@Override
	public TableSchema getTableSchema() {
		return schema;
	}

	@Override
	public TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		throw new UnsupportedOperationException(
			"This sink is configured by passing in a static schema when constructing");
	}

	public int getMaxResultsPerBatch() {
		return maxResultsPerBatch;
	}

	public boolean isCheckpointed() {
		return checkpointed;
	}

	/**
	 * Get the iterator for iterating through the query results.
	 *
	 * <p>NOTE: This iterator is incomplete. It must be configured with a job client before first use.
	 */
	public TableCollectIterator getIterator() {
		return iterator;
	}

	/**
	 * Set the iterator for iterating through the query results.
	 *
	 * <p>NOTE: This method is only called by the planner.
	 */
	public void setIterator(TableCollectIterator iterator) {
		this.iterator = iterator;
	}
}

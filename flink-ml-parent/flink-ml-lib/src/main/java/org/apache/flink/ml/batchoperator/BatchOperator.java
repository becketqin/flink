/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.batchoperator;

import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.batchoperator.source.TableSourceBatchOp;
import org.apache.flink.ml.common.AlgoOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class of batch algorithm operators.
 */
public abstract class BatchOperator<T extends BatchOperator<T>> extends AlgoOperator<T> {

	public BatchOperator() {
		super();
	}

	public BatchOperator(Params params) {
		super(params);
	}

	public <B extends BatchOperator<?>> B link(B next) {
		return linkTo(next);
	}

	public <B extends BatchOperator<?>> B linkTo(B next) {
		next.linkFrom(this);
		return next;
	}

	public abstract T linkFrom(BatchOperator<?>... inputs);

	@Override
	public String toString() {
		return getOutput().toString();
	}

	public static BatchOperator<?> sourceFrom(Table table) {
		return new TableSourceBatchOp(table);
	}

	protected void checkOpSize(int size, BatchOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length == size, "The size of operators should be equal to "
			+ size + ", current: " + inputs.length);
	}

	protected void checkRequiredOpSize(int size, BatchOperator<?>... inputs) {
		Preconditions.checkNotNull(inputs, "Operators should not be null.");
		Preconditions.checkState(inputs.length >= size, "The size of operators should be equal or greater than "
			+ size + ", current: " + inputs.length);
	}

	protected BatchOperator<?> checkAndGetFirst(BatchOperator<?> ... inputs) {
		checkOpSize(1, inputs);
		return inputs[0];
	}
}

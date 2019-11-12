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

package org.apache.flink.impl.connector.source.enumerator;

import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.api.connectors.source.SplitEnumeratorContext;
import org.apache.flink.api.connectors.source.event.SourceEvent;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * A class that adapts {@link InputSplitSource} to {@link SplitEnumerator}.
 *
 * @param <T> The concrete type of InputSplit.
 */
public final class InputSplitSourceWrapper<T extends InputSplit>
		implements SplitEnumerator<InputSplitWrapper<T>, List<InputSplitWrapper<T>>> {
	private final InputSplitSource<T> inputSplitSource;
	private final InputSplitAssigner assigner;
	private SplitEnumeratorContext<InputSplitWrapper<T>> context;

	public InputSplitSourceWrapper(InputSplitSource<T> inputSplitSource, int minNumTasks) throws Exception {
		this.inputSplitSource = inputSplitSource;
		this.assigner = inputSplitSource.getInputSplitAssigner(inputSplitSource.createInputSplits(minNumTasks));
	}

	@Override
	public void setSplitEnumeratorContext(SplitEnumeratorContext<InputSplitWrapper<T>> context) {
		this.context = context;
	}

	@Override
	public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
		throw new UnsupportedOperationException(String.format(
				"The InputSplitSourceWrapper received source event %s from subtask %d. " +
				"It should never receive any source event.",
				sourceEvent, subtaskId));
	}

	@Override
	public void addSplitsBack(List<InputSplitWrapper<T>> splits, int subtaskId) {
		assigner.returnInputSplit(convertList(splits, InputSplitWrapper::getSplit), subtaskId);
	}

	@Override
	public void addReader(int subtaskId) {
		throw new UnsupportedOperationException("The InputSplitSourceWrapper should never see this invocation.");
	}

	@Override
	public List<InputSplitWrapper<T>> snapshotState() {
		// No need to do snapshot. The splits will be recreated upon failover.
		return Collections.emptyList();
	}

	@Override
	public void restoreState(List<InputSplitWrapper<T>> checkpoint) {
		// Do nothing. The splits will be recreated upon failover.
	}

	@Override
	public void close() {
		// Do nothing.
	}

	private static <S, T> List<T> convertList(List<S> orig, Function<S, T> converter) {
		List<T> res = new ArrayList<>(orig.size());
		orig.forEach(s -> res.add(converter.apply(s)));
		return res;
	}
}

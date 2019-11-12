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

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.core.io.InputSplit;

/**
 * A wrapper class to adapt {@link InputSplit} to {@link SourceSplit}.
 *
 * @param <T> The concrete type of InputSplit.
 */
public final class InputSplitWrapper<T extends InputSplit> implements SourceSplit {
	private final T split;

	private InputSplitWrapper(T split) {
		this.split = split;
	}

	@Override
	public String splitId() {
		return Integer.toString(split.getSplitNumber());
	}

	public T getSplit() {
		return split;
	}
}

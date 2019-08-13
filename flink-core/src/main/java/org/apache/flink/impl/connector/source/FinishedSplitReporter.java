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

package org.apache.flink.impl.connector.source;

import java.util.function.Consumer;

/**
 * Reporter for {@link SplitFetcher} to pass the finished splits to the main thread.
 */
public class FinishedSplitReporter {
	private Consumer<String> finishedSplitsConsumer;

	/**
	 * Constructor.
	 *
	 * @param finishedSplitsConsumer the consumer to handle finished splits.
	 */
	FinishedSplitReporter(Consumer<String> finishedSplitsConsumer) {
		this.finishedSplitsConsumer = finishedSplitsConsumer;
	}

	/**
	 * Add a finished split.
	 *
	 * @param splitId the ID of the finished split.
	 */
	public void reportFinishedSplit(String splitId) {
		finishedSplitsConsumer.accept(splitId);
	}

	/**
	 * Chain an action to handle the finished splits.
	 *
	 * @param chainedFinishedSplitsConsumer a chained action to handle the finished splits.
	 */
	public void chainAction(Consumer<String> chainedFinishedSplitsConsumer) {
		finishedSplitsConsumer = finishedSplitsConsumer.andThen(chainedFinishedSplitsConsumer);
	}
}

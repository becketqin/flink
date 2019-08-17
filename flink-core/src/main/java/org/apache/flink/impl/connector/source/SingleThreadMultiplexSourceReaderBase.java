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

import org.apache.flink.api.connectors.source.SourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.fetcher.SingleThreadFetcherManager;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.synchronization.FutureNotifier;

import java.util.function.Supplier;

public abstract class SingleThreadMultiplexSourceReaderBase<E, T, SplitT extends SourceSplit, SplitStateT>
	extends SourceReaderBase<E, T, SplitT, SplitStateT> {

	public SingleThreadMultiplexSourceReaderBase(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
			Supplier<SplitReader<E, SplitT>> splitFetcherSupplier,
			RecordEmitter<E, T, SplitStateT> recordEmitter) {
		super(futureNotifier,
			  elementsQueue,
			  new SingleThreadFetcherManager<>(futureNotifier, elementsQueue, splitFetcherSupplier),
			  recordEmitter);
	}

	@Override
	public void configure(Configuration config) {
		super.configure(config);
		splitFetcherManager.configure(config);
		recordEmitter.configure(config);
	}
}

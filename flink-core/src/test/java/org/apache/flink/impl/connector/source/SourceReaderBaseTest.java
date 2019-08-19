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

import org.apache.flink.api.connectors.source.Boundedness;
import org.apache.flink.impl.connector.source.mocks.MockSourceReader;
import org.apache.flink.impl.connector.source.mocks.MockSplit;
import org.apache.flink.impl.connector.source.mocks.MockSplitReader;
import org.apache.flink.impl.connector.source.splitreader.SplitReader;
import org.apache.flink.impl.connector.source.splitreader.SplitsChange;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.impl.connector.source.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.synchronization.FutureNotifier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

/**
 * A unit test class for {@link SourceReaderBase}
 */
public class SourceReaderBaseTest extends SourceReaderTest<MockSplit> {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testExceptionInSplitReader() throws InterruptedException {
		expectedException.expect(RuntimeException.class);
		expectedException.expectMessage("One or more fetchers have encountered exception");
		final String errMsg = "Testing Exception";

		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		MockSourceReader reader = new MockSourceReader(
				futureNotifier,
				elementsQueue,
				() -> new SplitReader<int[], MockSplit>() {
					@Override
					public RecordsWithSplitIds<int[]> fetch() {
						throw new RuntimeException(errMsg);
					}

					@Override
					public void handleSplitsChanges(Queue<SplitsChange<MockSplit>> splitsChanges) {
						// We have to handle split changes first, otherwise fetch will not be called.
						splitsChanges.clear();
					}

					@Override
					public void wakeUp() {}

					@Override
					public void configure(Configuration config) {}
				});


		reader.configure(getConfig(Boundedness.BOUNDED));
		ValidatingSourceOutput output = new ValidatingSourceOutput();
		reader.addSplits(Collections.singletonList(getSplit(0, NUM_RECORDS_PER_SPLIT, Boundedness.UNBOUNDED)));
		// This is not a real infinite loop, it is supposed to throw exception after two polls.
		while (true) {
			reader.pollNext(output);
			// Add a sleep to avoid tight loop.
			Thread.sleep(1);
		}
	}

	// ---------------- helper methods -----------------

	@Override
	protected MockSourceReader createReader(Boundedness boundedness) {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		MockSplitReader mockSplitReader =
				new MockSplitReader(2, true, true);
		MockSourceReader reader = new MockSourceReader(
				futureNotifier,
				elementsQueue,
				() -> mockSplitReader);
		Configuration config = getConfig(boundedness);
		reader.configure(config);
		return reader;
	}

	@Override
	protected List<MockSplit> getSplits(int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
		List<MockSplit> mockSplits = new ArrayList<>();
		for (int i = 0; i < numSplits; i++) {
			mockSplits.add(getSplit(i, numRecordsPerSplit, boundedness));
		}
		return mockSplits;
	}

	@Override
	protected MockSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
		MockSplit mockSplit;
		if (boundedness == Boundedness.BOUNDED) {
			mockSplit = new MockSplit(splitId, 0, numRecords);
		} else {
			mockSplit = new MockSplit(splitId);
		}
		for (int j = 0; j < numRecords; j++) {
			mockSplit.addRecord(splitId * 10 + j);
		}
		return mockSplit;
	}

	@Override
	protected long getIndex(MockSplit split) {
		return split.index();
	}

	private Configuration getConfig(Boundedness boundedness) {
		Configuration config = new Configuration();
		config.setInteger(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY, 1);
		config.setLong(SourceReaderOptions.SOURCE_READER_CLOSE_TIMEOUT, 30000L);
		config.setString(SourceReaderOptions.SOURCE_READER_BOUNDEDNESS, boundedness.name());
		return config;
	}
}

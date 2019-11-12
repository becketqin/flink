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

package org.apache.flink.streaming.connectors.kafka.newsrc;

import org.apache.flink.api.connectors.source.Source;
import org.apache.flink.api.connectors.source.SourceContext;
import org.apache.flink.api.connectors.source.SourceReader;
import org.apache.flink.api.connectors.source.SplitEnumerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.impl.connector.source.reader.RecordsWithSplitIds;
import org.apache.flink.impl.connector.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.impl.connector.source.reader.synchronization.FutureNotifier;
import org.apache.flink.streaming.connectors.kafka.newsrc.enumerator.KafkaPartitionEnumerator;
import org.apache.flink.streaming.connectors.kafka.newsrc.enumerator.KafkaPartitionsCheckpoint;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * The Kafka source class.
 */
public class KafkaSource<K, V, T> implements Source<T, KafkaPartition, KafkaPartitionsCheckpoint> {

	@Override
	public SourceReader createReader(Configuration config) {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<ConsumerRecord<K, V>>> elementQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		return new KafkaSourceReader<>(
				futureNotifier,
				elementQueue,
				() -> new KafkaPartitionReader<>(config),
				new ConsumerRecordEmitter<>(),
				config);
	}

	@Override
	public SplitEnumerator<KafkaPartition, KafkaPartitionsCheckpoint> createEnumerator(Configuration config) {
		return new KafkaPartitionEnumerator(config);
	}

	@Override
	public SplitEnumerator<KafkaPartition, KafkaPartitionsCheckpoint> restoreEnumerator(
			Configuration config,
			KafkaPartitionsCheckpoint checkpoint) {
		KafkaPartitionEnumerator enumerator = new KafkaPartitionEnumerator(config);
		enumerator.restoreState(checkpoint);
		return enumerator;
	}

	@Override
	public SimpleVersionedSerializer<KafkaPartition> getSplitSerializer() {
		return null;
	}

	@Override
	public SimpleVersionedSerializer<KafkaPartitionsCheckpoint> getEnumeratorCheckpointSerializer() {
		return null;
	}
}

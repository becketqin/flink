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

package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.connectors.metrics.SourceMetrics;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricDef;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricSpec;

/**
 * A class that holds all Kafka Source Metrics.
 */
public class KafkaSourceMetrics extends SourceMetrics {

	public static final String KAFKA_CONSUMER_METRICS_GROUP = "KafkaConsumer";

	public static final String COMMITS_SUCCEEDED_METRICS_COUNTER = "commitsSucceeded";
	private static final String COMMITS_SUCCEEDED_METRICS_COUNTER_DOC = "The total number of successful offset commits.";

	public static final String COMMITS_FAILED_METRICS_COUNTER = "commitsFailed";
	private static final String COMMITS_FAILED_METRICS_COUNTER_DOC = "The total number of failed offset commits.";

	private static final MetricDef METRIC_DEF = new MetricDef()
		.define(
			COMMITS_SUCCEEDED_METRICS_COUNTER,
			COMMITS_SUCCEEDED_METRICS_COUNTER_DOC,
			MetricSpec.counter())
		.define(
			COMMITS_FAILED_METRICS_COUNTER,
			COMMITS_FAILED_METRICS_COUNTER_DOC,
			MetricSpec.counter());

	// initialize commit metrics and default offset callback method
	public final Counter successfulCommits;
	public final Counter failedCommits;

	public KafkaSourceMetrics(MetricGroup metricGroup) {
		super(metricGroup.addGroup(KAFKA_CONSUMER_METRICS_GROUP), METRIC_DEF);

		successfulCommits = get(COMMITS_SUCCEEDED_METRICS_COUNTER);
		failedCommits = get(COMMITS_FAILED_METRICS_COUNTER);
	}
}

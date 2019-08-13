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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

public class SourceReaderOptions {

	public static final ConfigOption<Long> SOURCE_READER_CLOSE_TIMEOUT =
		ConfigOptions.key("source.reader.close.timeout")
					 .defaultValue(30000L)
					 .withDescription("The timeout when close the source reader");

	public static final ConfigOption<String> SOURCE_READER_BOUNDEDNESS =
		ConfigOptions.key("boundedness")
					 .noDefaultValue()
					 .withDescription("The boundedness of the source.");

	public static final ConfigOption<Integer> ELEMENT_QUEUE_CAPACITY =
		ConfigOptions.key("source.reader.element.queue.capacity")
					 .defaultValue(100)
					 .withDescription("The capacity of the element queue in the source reader.");

	// --------------- final fields ----------------------
	public final long sourceReaderCloseTimeout;
	public final Boundedness boundedness;
	public final int elementQueueCapacity;

	public SourceReaderOptions(Configuration config) {
		this.sourceReaderCloseTimeout = config.getLong(SOURCE_READER_CLOSE_TIMEOUT);
		this.boundedness = config.getEnum(Boundedness.class, SOURCE_READER_BOUNDEDNESS);
		this.elementQueueCapacity = config.getInteger(ELEMENT_QUEUE_CAPACITY);
	}
}

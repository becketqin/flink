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

package org.apache.flink.api.common.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.connectors.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A class that unifies the functionality of InputFormat and SourceFunction and Source in runtime.
 * This class contains either a {@link Source} or an {@link InputFormat}. When it contains a
 * Source, only the methods for Source are expected to be called. Otherwise, only the
 * methods for InputFormat is expected to be called.
 */
@Internal
public class UnifiedSource<OUT, T extends InputSplit> implements InputFormat<OUT, T> {
	private final InputFormat<OUT, T> inputFormat;
	private final Source<OUT, ?, ?> source;

	/**
	 * Construct a UnifiedSource from a InputFormat.
	 */
	public UnifiedSource(InputFormat<OUT, T> inputFormat) {
		this.inputFormat = inputFormat;
		this.source = null;
	}

	/**
	 * Construct a UnifiedSource from a Source.
	 */
	public UnifiedSource(Source<OUT, ?, ?> source) {
		this.inputFormat = null;
		this.source = source;
	}

	/**
	 * Get the {@link Source} in this UnifiedSource.
	 *
	 * @return The {@link Source} in this UnifiedSource.
	 */
	public Source<OUT, ?, ?> getsSource() {
		return source;
	}

	// *********************************************************************************************
	// * Methods from the InputFormat. These methods are only called when inputFormat is not null. *
	// *********************************************************************************************

	@Override
	public void configure(Configuration parameters) {
		Preconditions.checkNotNull(inputFormat, "Null input format.");
		inputFormat.configure(parameters);
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		Preconditions.checkNotNull(inputFormat, "Null input format.");
		return inputFormat.getStatistics(cachedStatistics);
	}

	@Override
	public T[] createInputSplits(int minNumSplits) throws IOException {
		// This method is called in the job master.
		return inputFormat == null ? null : inputFormat.createInputSplits(minNumSplits);
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(T[] inputSplits) {
		// This method is called in the job master.
		return inputFormat == null ? null : inputFormat.getInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(T split) throws IOException {
		Preconditions.checkNotNull(inputFormat, "Null input format.");
		inputFormat.open(split);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		Preconditions.checkNotNull(inputFormat, "Null input format.");
		return inputFormat.reachedEnd();
	}

	@Override
	public OUT nextRecord(OUT reuse) throws IOException {
		Preconditions.checkNotNull(inputFormat, "Null input format.");
		return inputFormat.nextRecord(reuse);
	}

	@Override
	public void close() throws IOException {
		Preconditions.checkNotNull(inputFormat, "Null input format.");
		inputFormat.close();
	}
}

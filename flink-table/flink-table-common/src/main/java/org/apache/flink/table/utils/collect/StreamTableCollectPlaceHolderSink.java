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

package org.apache.flink.table.utils.collect;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * A place holder sink for retrieving query results of batch jobs to the client.
 * It will be changed into dedicated sink operators in planners.
 */
@Internal
public class StreamTableCollectPlaceHolderSink extends AbstractTableCollectPlaceHolderSink<Tuple2<Boolean, Row>> {

	public StreamTableCollectPlaceHolderSink(
			TableSchema schema,
			int maxResultsPerBatch,
			boolean checkpointed) {
		super(schema, maxResultsPerBatch, checkpointed);
	}

	@Override
	public TypeInformation<Tuple2<Boolean, Row>> getOutputType() {
		return new TupleTypeInfo<>(Types.BOOLEAN, schema.toRowType());
	}
}

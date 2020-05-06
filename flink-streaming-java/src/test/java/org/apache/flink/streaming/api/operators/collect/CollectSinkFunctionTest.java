/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link CollectSinkFunction}.
 */
public class CollectSinkFunctionTest {

	private static final int MAX_RESULTS_PER_BATCH = 3;
	private static final String LIST_ACC_NAME = "tableCollectList";
	private static final String TOKEN_ACC_NAME = "tableCollectToken";

	CollectSinkFunction<Row> function;
	CollectSinkOperatorCoordinator coordinator;

	private TypeSerializer<Row> serializer;
	private StreamingRuntimeContext runtimeContext;
	private MockOperatorEventGateway gateway;

	@Before
	public void before() throws Exception {
		serializer = new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO).createSerializer(new ExecutionConfig());
		runtimeContext = new MockStreamingRuntimeContext(false, 1, 0);
		gateway = new MockOperatorEventGateway();
		function = new CollectSinkFunction<>(serializer, MAX_RESULTS_PER_BATCH, LIST_ACC_NAME, TOKEN_ACC_NAME);
		coordinator = new CollectSinkOperatorCoordinator();

		function.setRuntimeContext(runtimeContext);
		function.setOperatorEventGateway(gateway);
		function.open(new Configuration());
		coordinator.handleEventFromOperator(0, gateway.getEventsSent().get(0));
	}

	@Test
	public void testProtocol() throws Exception {
		for (int i = 0; i < 6; i++) {
			// CollectSinkFunction never use context when invoked
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse.DeserializedResponse<Row> response1 = sendRequest("", 0);
		Assert.assertEquals(response1.getToken(), 0);
		String version = response1.getVersion();

		CollectCoordinationResponse.DeserializedResponse<Row> response2 = sendRequest(version, 0);
		assertResponseEquals(response2, version, 0, Arrays.asList(0, 1, 2));

		CollectCoordinationResponse.DeserializedResponse<Row> response3 = sendRequest(version, 4);
		assertResponseEquals(response3, version, 4, Arrays.asList(4, 5));

		CollectCoordinationResponse.DeserializedResponse<Row> response4 = sendRequest(version, 6);
		assertResponseEquals(response4, version, 6, Collections.emptyList());

		for (int i = 6; i < 10; i++) {
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse.DeserializedResponse<Row> response5 = sendRequest(version, 5);
		assertResponseEquals(response5, version, 6, Collections.emptyList());

		CollectCoordinationResponse.DeserializedResponse<Row> response6 = sendRequest(version, 6);
		assertResponseEquals(response6, version, 6, Arrays.asList(6, 7, 8));

		CollectCoordinationResponse.DeserializedResponse<Row> response7 = sendRequest(version, 6);
		assertResponseEquals(response7, version, 6, Arrays.asList(6, 7, 8));

		function.close();
		Accumulator listAccumulator = runtimeContext.getAccumulator(LIST_ACC_NAME);
		ArrayList<byte[]> serializedResult = ((SerializedListAccumulator) listAccumulator).getLocalValue();
		List<Row> accResult = SerializedListAccumulator.deserializeList(serializedResult, serializer);
		Assert.assertEquals(4, accResult.size());
		for (int i = 0; i < 4; i++) {
			Row row = accResult.get(i);
			Assert.assertEquals(1, row.getArity());
			Assert.assertEquals(i + 6, row.getField(0));
		}
		Accumulator tokenAccumulator = runtimeContext.getAccumulator(TOKEN_ACC_NAME);
		long token = ((LongCounter) tokenAccumulator).getLocalValue();
		Assert.assertEquals(6, token);
	}

	private CollectCoordinationResponse.DeserializedResponse<Row> sendRequest(String version, long token) {
		CollectCoordinationRequest request = new CollectCoordinationRequest(version, token);
		return ((CollectCoordinationResponse) coordinator.handleCoordinationRequest(request))
			.getDeserializedResponse(serializer);
	}

	private void assertResponseEquals(
			CollectCoordinationResponse.DeserializedResponse<Row> response,
			String version,
			long token,
			List<Integer> expected) {
		Assert.assertEquals(version, response.getVersion());
		Assert.assertEquals(token, response.getToken());
		Assert.assertEquals(expected.size(), response.getResults().size());
		for (int i = 0; i < expected.size(); i++) {
			Row row = response.getResults().get(i);
			Assert.assertEquals(1, row.getArity());
			Assert.assertEquals(expected.get(i), row.getField(0));
		}
	}
}

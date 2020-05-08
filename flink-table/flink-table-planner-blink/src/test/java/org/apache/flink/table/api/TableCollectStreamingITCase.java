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

package org.apache.flink.table.api;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.planner.runtime.utils.FailingCollectionSource;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * IT case for TableEnvironment#collect in streaming mode.
 */
public class TableCollectStreamingITCase {

	private static StreamTableEnvironment tEnv;

	@BeforeClass
	public static void beforeClass() throws IOException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(3);
		env.enableCheckpointing(100, CheckpointingMode.EXACTLY_ONCE);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		tEnv = StreamTableEnvironment.create(env, settings);

		tEnv.getConfig().getConfiguration().setInteger(
			ExecutionConfigOptions.TABLE_EXEC_COLLECT_MAX_BATCH_SIZE, 2);

		RowTypeInfo rowTypeInfo = getRowTypeInfo();
		FailingCollectionSource<Row> source = new FailingCollectionSource<>(
			rowTypeInfo.createSerializer(new ExecutionConfig()), getData(), 5);
		tEnv.registerTable("T", tEnv.fromDataStream(
			env.addSource(source).returns(rowTypeInfo).setParallelism(1), "a, b"));
	}

	@After
	public void after() {
		FailingCollectionSource.reset();
	}

	@Test
	public void testCheckpointed() throws Exception {
		tEnv.getConfig().getConfiguration().setBoolean(
			ExecutionConfigOptions.TABLE_EXEC_COLLECT_EXACTLY_ONCE, true);

		for (int i = 0; i < 2; i++) {
			// run multiple times to make sure no errors will occur
			// when the method is called a second time
			runQuery();
			FailingCollectionSource.reset();
		}
	}

	@Test(expected = RuntimeException.class)
	public void testUncheckpointed() throws Exception {
		tEnv.getConfig().getConfiguration().setBoolean(
			ExecutionConfigOptions.TABLE_EXEC_COLLECT_EXACTLY_ONCE, false);

		try {
			runQuery();
		} catch (RuntimeException e) {
			Assert.assertTrue(e.getMessage().contains("Table collect sink has restarted"));
			throw e;
		}
		Assert.fail("Expecting a RuntimeException");
	}

	private void runQuery() throws Exception {
		Table table = tEnv.sqlQuery("SELECT * FROM T");
		Iterator<Tuple2<Boolean, Row>> iterator = tEnv.collect(table);

		Iterable<Row> expected = getData();
		for (Row row : expected) {
			Assert.assertTrue(iterator.hasNext());
			Tuple2<Boolean, Row> actual = iterator.next();
			Assert.assertTrue(actual.f0);
			Assert.assertEquals(row.getArity(), actual.f1.getArity());
			for (int i = 0; i < row.getArity(); i++) {
				Assert.assertEquals(row.getField(i), actual.f1.getField(i));
			}
		}
		Assert.assertFalse(iterator.hasNext());
	}

	private static RowTypeInfo getRowTypeInfo() {
		return new RowTypeInfo(
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO);
	}

	private static Iterable<Row> getData() {
		return Arrays.asList(
			Row.of(1, "String A"),
			Row.of(2, "String BB"),
			Row.of(3, "String CCC"),
			Row.of(4, "String DDDD"),
			Row.of(5, "String EEEEE"),
			Row.of(6, "String FFFFFF"),
			Row.of(7, "String GGGGGGG"),
			Row.of(8, "String HHHHHHHH"),
			Row.of(9, "String IIIIIIIII"),
			Row.of(10, "String JJJJJJJJJJ"));
	}
}

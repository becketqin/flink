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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * IT case for TableEnvironment#collect in batch mode.
 */
public class TableCollectBatchITCase extends BatchTestBase {

	@Test
	public void testCollect() throws Exception {
		tEnv().getConfig().getConfiguration().setInteger(
			ExecutionConfigOptions.TABLE_EXEC_COLLECT_MAX_BATCH_SIZE, 2);
		tEnv().getConfig().getConfiguration().setBoolean(
			ExecutionConfigOptions.TABLE_EXEC_COLLECT_EXACTLY_ONCE, false);

		List<Row> sourceData = Arrays.asList(
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
		registerJavaCollection(
			"T",
			sourceData,
			new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
			"a, b");

		// run multiple times to make sure no errors will occur
		// when the method is called a second time
		for (int i = 0; i < 2; i++) {
			Table table = tEnv().sqlQuery("SELECT * FROM T ORDER BY a");
			Iterator<Tuple2<Boolean, Row>> iterator = tEnv().collect(table);
			for (Row expected : sourceData) {
				Assert.assertTrue(iterator.hasNext());
				Tuple2<Boolean, Row> actual = iterator.next();
				Assert.assertTrue(actual.f0);
				Assert.assertEquals(expected.getArity(), actual.f1.getArity());
				for (int j = 0; j < expected.getArity(); j++) {
					Assert.assertEquals(expected.getField(j), actual.f1.getField(j));
				}
			}
			Assert.assertFalse(iterator.hasNext());
		}
	}
}

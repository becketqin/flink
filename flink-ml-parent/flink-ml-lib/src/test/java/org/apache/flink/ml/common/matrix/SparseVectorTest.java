/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.matrix;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases for SparseVector.
 */
public class SparseVectorTest {
	private SparseVector v1 = null;
	private SparseVector v2 = null;

	private static final double TOL = 1.0e-6;

	@Before
	public void setUp() throws Exception {
		{
			int n = 8;
			int[] indices = new int[]{1, 3, 5, 7};
			double[] values = new double[]{2.0, 2.0, 2.0, 2.0};
			v1 = new SparseVector(n, indices, values);
		}

		{
			int n = 8;
			int[] indices = new int[]{3, 4, 5};
			double[] values = new double[]{1.0, 1.0, 1.0};
			v2 = new SparseVector(n, indices, values);
		}
	}

	@Test
	public void size() throws Exception {
		Assert.assertEquals(v1.size(), 8);
	}

	@Test
	public void prefix() throws Exception {
		SparseVector prefixed = v1.prefix(0.2);
		Assert.assertArrayEquals(prefixed.getIndices(), new int[]{0, 2, 4, 6, 8});
		Assert.assertArrayEquals(prefixed.getValues(), new double[]{0.2, 2, 2, 2, 2}, 0);
	}

	@Test
	public void append() throws Exception {
		SparseVector prefixed = v1.append(0.2);
		Assert.assertArrayEquals(prefixed.getIndices(), new int[]{1, 3, 5, 7, 8});
		Assert.assertArrayEquals(prefixed.getValues(), new double[]{2, 2, 2, 2, 0.2}, 0);
	}

	@Test
	public void normL2Square() throws Exception {
		Assert.assertEquals(v2.normL2Square(), 3.0, TOL);
	}

	@Test
	public void minus() throws Exception {
		SparseVector d = v1.minus(v2);
		Assert.assertEquals(d.get(0), 0.0, TOL);
		Assert.assertEquals(d.get(1), 2.0, TOL);
		Assert.assertEquals(d.get(2), 0.0, TOL);
		Assert.assertEquals(d.get(3), 1.0, TOL);
		Assert.assertEquals(d.get(4), -1.0, TOL);
	}

	@Test
	public void plus() throws Exception {
		SparseVector d = v1.plus(v2);
		Assert.assertEquals(d.get(0), 0.0, TOL);
		Assert.assertEquals(d.get(1), 2.0, TOL);
		Assert.assertEquals(d.get(2), 0.0, TOL);
		Assert.assertEquals(d.get(3), 3.0, TOL);
	}

	@Test
	public void dot() throws Exception {
		Assert.assertEquals(v1.dot(v2), 4.0, TOL);
	}

	@Test
	public void get() throws Exception {
		Assert.assertEquals(v1.get(5), 2.0, TOL);
		Assert.assertEquals(v1.get(6), 0.0, TOL);
	}

	@Test
	public void slice() throws Exception {
		int n = 8;
		int[] indices = new int[]{1, 3, 5, 7};
		double[] values = new double[]{2.0, 3.0, 4.0, 5.0};
		SparseVector v = new SparseVector(n, indices, values);

		int[] indices1 = new int[]{5, 4, 3};
		SparseVector vec1 = v.slice(indices1);
		Assert.assertEquals(vec1.n, 3);
		Assert.assertArrayEquals(vec1.indices, new int[]{0, 2});
		Assert.assertArrayEquals(vec1.values, new double[]{4.0, 3.0}, 0.0);

		int[] indices2 = new int[]{3, 5};
		SparseVector vec2 = v.slice(indices2);
		Assert.assertArrayEquals(vec2.indices, new int[]{0, 1});
		Assert.assertArrayEquals(vec2.values, new double[]{3.0, 4.0}, 0.0);

		int[] indices3 = new int[]{2, 4};
		SparseVector vec3 = v.slice(indices3);
		Assert.assertEquals(vec3.n, 2);
		Assert.assertArrayEquals(vec3.indices, new int[]{});
		Assert.assertArrayEquals(vec3.values, new double[]{}, 0.0);

		int[] indices4 = new int[]{2, 2, 4, 4};
		SparseVector vec4 = v.slice(indices4);
		Assert.assertEquals(vec4.n, 4);
		Assert.assertArrayEquals(vec4.indices, new int[]{});
		Assert.assertArrayEquals(vec4.values, new double[]{}, 0.0);
	}

	@Test
	public void serialize() throws Exception {
		Assert.assertEquals(v1.serialize(), "$8$1:2.0,3:2.0,5:2.0,7:2.0");
	}

	@Test
	public void deserialize() throws Exception {
		SparseVector vec1 = SparseVector.deserialize("0:1,2:-3");
		SparseVector vec3 = SparseVector.deserialize("$4$0:1,2:-3");
		SparseVector vec4 = SparseVector.deserialize("$4$");
		SparseVector vec5 = SparseVector.deserialize("");
		Assert.assertEquals(vec1.get(0), 1., 0.);
		Assert.assertEquals(vec1.get(2), -3., 0.);
		Assert.assertArrayEquals(vec3.toDenseVector().getData(), new double[]{1, 0, -3, 0}, 0);
		Assert.assertEquals(vec3.n, 4);
		Assert.assertArrayEquals(vec4.toDenseVector().getData(), new double[]{0, 0, 0, 0}, 0);
		Assert.assertEquals(vec4.n, 4);
		Assert.assertEquals(vec5.n, -1);
	}

}

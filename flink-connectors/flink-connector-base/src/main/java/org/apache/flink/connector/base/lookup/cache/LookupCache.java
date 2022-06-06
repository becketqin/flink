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

package org.apache.flink.connector.base.lookup.cache;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;

import java.util.Collection;

public interface LookupCache {
    /**
     * Initialize the cache.
     *
     * @param metricGroup the metric group to register cache related metrics.
     */
    void open(MetricGroup metricGroup);

    /**
     * Returns the value associated with key in this cache, or null if there is no cached value for
     * key.
     */
    Collection<RowData> getIfPresent(RowData key);

    /**
     * Associates the specified value rows with the specified key row in the cache. If the cache
     * previously contained value associated with the key, the old value is replaced by the
     * specified value.
     *
     * @return the previous value rows associated with key, or null if there was no mapping for key.
     * @param key - key row with which the specified value is to be associated
     * @param value – value rows to be associated with the specified key
     */
    Collection<RowData> put(RowData key, Collection<RowData> value);

    /** Discards any cached value for the specified key. */
    void invalidate(RowData key);

    void clear();

    /** Returns the number of key-value mappings in the cache. */
    long size();
}

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

package org.apache.flink.connector.base.lookup;

import org.apache.flink.connector.base.lookup.cache.CacheMaintainer;
import org.apache.flink.connector.base.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;

/**
 * The Flink framework only provides the primitive capabilities, without assuming the
 * use case of such capability.
 *
 * The Flink framework in this case, interact with the framework in the following way:
 * 1. The lookup case. This always goes through either {@link LookupFunction} or
 *    {@link AsyncLookupFunction}.
 * 2. The cache loading case. The Flink frameworks provides the capability to populate
 *    the LookupFunction cache with a ScanTableSource. All such cache loadings are
 *    initiated in the LookupFunction by invoking the
 *    {@link CacheMaintainer.Context#loadCache()} method.
 *
 * With the above two key design primitives, the abstract LookupFunction can support
 * various flavors of caching strategy. For example,
 *
 * 1. No caching. Users can create a no caching lookup function by passing a NULL to
 *    the constructor of this class.
 * 2. Partial caching. If users construct the LookupFunction with a non-null LookupCache,
 *    this is the default behavior of the LookupFunction. The eval() method will first
 *    check the LookupCache to see if a key-value pair has been cached. If not, it invokes
 *    the {@link #lookup(RowData)} method to retrieve it and then put it into the cache.
 * 3. Full caching. If users wants to populate the cache upfront with rows from an
 *    external table, they can to the following:
 *    a. Override the {@link LookupFunctionProvider#getScanTableCacheLoader()} method to return a proper
 *    {@link org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider
 *    ScanRuntimeCacheLoader}. The {@link #initializeCache(Context)} method will use
 *    the ScanTableSource to populate the cache before processing any data.
 *    b. Users can also implement their own policy to reload the cache if they want to.
 *    c. User can choose to implement the {@link #lookup(RowData)} method in different ways to
 *       handle the case that a key is missing from the full caching. e.g.
 *       i. Issue a remote lookup and the result will be put into the cache.
 *       ii. Throw an IllegalStateException if no cache missing is expected after the cache
 *           initialization.
 *       iii. Return a default value when a key is missing.
 */
public abstract class LookupFunction extends TableFunction<RowData> implements CacheMaintainer {
    protected LookupCache lookupCache;
    protected final boolean cacheEnabled;

    public LookupFunction(@Nullable LookupCache lookupCache) {
        this.cacheEnabled = lookupCache == null;
        this.lookupCache = lookupCache;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    /**
     * Synchronously lookup rows matching the lookup keys.
     *
     * @param keyRow - A {@link RowData} that wraps keys to lookup.
     * @return A collections of all matching rows in the lookup table.
     */
    public abstract Collection<RowData> lookup(RowData keyRow) throws IOException;

    /** Invoke {@link #lookup} and handle exceptions. */
    public final void eval(Object... keys) {
        try {
            if (cacheEnabled)  {
                RowData keyRow = GenericRowData.of(keys);
                Collection<RowData> value = lookupCache.getIfPresent(keyRow);
                if (value == null) {
                    value = lookup(keyRow);
                    maybeCache(keyRow, value);
                }
                value.forEach(this::collect);
            } else {
                lookup(GenericRowData.of(keys)).forEach(this::collect);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to lookup values with given key", e);
        }
    }

    /**
     * Maybe merged with open() if we have a LookupFunctionContext.
     */
    @Override
    public void initializeCache(Context cacheContext) {
        cacheContext.loadCache();
    }

    @Override
    public void maybeCache(RowData key, Collection<RowData> value) {
        lookupCache.put(key, value);

    }

    @Override
    public void invalidateCache(RowData key) {
        lookupCache.invalidate(key);
    }

    @Override
    public void disposeCache() {
        lookupCache.clear();
    }
}

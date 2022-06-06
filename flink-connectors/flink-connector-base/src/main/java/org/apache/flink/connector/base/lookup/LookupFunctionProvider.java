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
import org.apache.flink.connector.base.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * From the Flink framework perspective, there are only two things that it needs to be aware of.
 * 1. Read the value corresponding to a key. And ideally, there should only be one way to do this.
 *    That is the eval() method in the LookupFunction.
 * 2. [Optional] Set up the ScanTableSource to populate the cache on demand. In order to do this,
 *    the Flink framework needs to have a ScanTableSource, and a cache maintainer API.
 */
public interface LookupFunctionProvider {

    LookupFunction getLookupFunction();

    ScanTableSource.ScanRuntimeProvider getScanTableCacheLoader();

    //-------------------------------------------------------------

    static LookupFunctionProvider withFullCache(
            ScanTableSource.ScanRuntimeProvider scanRuntimeProvider,
            Consumer<CacheMaintainer.Context> reloadTrigger) {
        LookupCache defaultCache = new DefaultLookupCache(
                Duration.ofDays(Long.MAX_VALUE),
                Duration.ofDays(Long.MAX_VALUE),
                Long.MAX_VALUE);
        return withFullCache(scanRuntimeProvider, reloadTrigger, defaultCache);
    }

    static LookupFunctionProvider withFullCache(
            ScanTableSource.ScanRuntimeProvider scanRuntimeProvider,
            Consumer<CacheMaintainer.Context> reloadTrigger,
            LookupCache lookupCache) {
        return new LookupFunctionProvider() {
            @Override
            public LookupFunction getLookupFunction() {
                return new LookupFunction(lookupCache) {
                    private LookupCache newCache;
                    private CacheContextWrapper wrappedCacheContext;

                    @Override
                    public Collection<RowData> lookup(RowData keyRow) throws IOException {
                        return null;
                    }

                    @Override
                    public void initializeCache(Context cacheContext) {
                        wrappedCacheContext = new CacheContextWrapper(cacheContext);
                        reloadTrigger.accept(wrappedCacheContext);
                    }

                    @Override
                    public void maybeCache(RowData key, Collection<RowData> value) {
                        // In full caching mode, this function is only called by the
                        // Flink framework.
                        newCache.put(key, value);
                    }

                    class CacheContextWrapper implements CacheMaintainer.Context {
                        private final CacheMaintainer.Context origContext;

                        CacheContextWrapper(CacheMaintainer.Context origContext) {
                            this.origContext = origContext;
                        }

                        @Override
                        public long currentProcessingTime() {
                            return origContext.currentProcessingTime();
                        }

                        @Override
                        public long currentWatermark() {
                            return origContext.currentWatermark();
                        }

                        @Override
                        public CompletableFuture<Void> loadCache() {
                            newCache = new DefaultLookupCache(
                                    Duration.ofDays(Long.MAX_VALUE),
                                    Duration.ofDays(Long.MAX_VALUE),
                                    Long.MAX_VALUE);
                            return origContext.loadCache().thenAccept(ignored -> lookupCache = newCache);
                        }
                    }
                };
            }

            @Override
            public ScanTableSource.ScanRuntimeProvider getScanTableCacheLoader() {
                return scanRuntimeProvider;
            }
        };
    }

    static LookupFunctionProvider withPartialCache(Function<RowData, Collection<RowData>> lookup) {
        LookupCache defaultCache = new DefaultLookupCache(
                Duration.ofDays(Long.MAX_VALUE),
                Duration.ofDays(Long.MAX_VALUE),
                Long.MAX_VALUE);
        return withPartialCache(lookup, defaultCache);
    }

    static LookupFunctionProvider withPartialCache(
            Function<RowData, Collection<RowData>> lookup,
            LookupCache lookupCache) {
        return new LookupFunctionProvider() {
            @Override
            public LookupFunction getLookupFunction() {
                return new LookupFunction(lookupCache) {
                    @Override
                    public Collection<RowData> lookup(RowData keyRow) throws IOException {
                        return lookup.apply(keyRow);
                    }

                    @Override
                    public void initializeCache(Context cacheContext) {}
                };
            }

            @Override
            public ScanTableSource.ScanRuntimeProvider getScanTableCacheLoader() {
                return null;
            }
        };
    }
}



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

import java.time.Duration;
import java.util.Collection;

/** Default implementation of {@link LookupCache}. */
public class DefaultLookupCache implements LookupCache {
    private final Duration expireAfterAccessDuration;
    private final Duration expireAfterWriteDuration;
    private final Long maximumSize;

    public DefaultLookupCache(
            Duration expireAfterAccessDuration,
            Duration expireAfterWriteDuration,
            Long maximumSize) {
        this.expireAfterAccessDuration = expireAfterAccessDuration;
        this.expireAfterWriteDuration = expireAfterWriteDuration;
        this.maximumSize = maximumSize;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public void open(MetricGroup metricGroup) {

    }

    @Override
    public Collection<RowData> getIfPresent(RowData key) {
        return null;
    }

    @Override
    public Collection<RowData> put(RowData key, Collection<RowData> value) {
        return null;
    }

    @Override
    public void invalidate(RowData key) {

    }

    @Override
    public void clear() {

    }

    @Override
    public long size() {
        return 0;
    }

    public static class Builder {
        private Duration expireAfterAccessDuration;
        private Duration expireAfterWriteDuration;
        private Long maximumSize;

        public DefaultLookupCache.Builder expireAfterAccess(Duration duration) {
            expireAfterAccessDuration = duration;
            return this;
        }

        public DefaultLookupCache.Builder expireAfterWrite(Duration duration) {
            expireAfterWriteDuration = duration;
            return this;
        }

        public DefaultLookupCache.Builder maximumSize(long maximumSize) {
            this.maximumSize = maximumSize;
            return this;
        }

        public DefaultLookupCache build() {
            return new DefaultLookupCache(
                    expireAfterAccessDuration,
                    expireAfterWriteDuration,
                    maximumSize);
        }
    }

}

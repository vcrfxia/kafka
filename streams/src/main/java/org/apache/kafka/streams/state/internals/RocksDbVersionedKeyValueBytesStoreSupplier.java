/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

public class RocksDbVersionedKeyValueBytesStoreSupplier implements VersionedBytesStoreSupplier {

    private final String name;
    private final long historyRetentionMs;
    private final long segmentIntervalMs;

    public RocksDbVersionedKeyValueBytesStoreSupplier(final String name,
                                                      final long historyRetentionMs,
                                                      final long segmentIntervalMs
    ) {
        this.name = name;
        this.historyRetentionMs = historyRetentionMs;
        this.segmentIntervalMs = segmentIntervalMs;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long historyRetentionMs() {
        return historyRetentionMs;
    }

    public long segmentIntervalMs() {
        return segmentIntervalMs;
    }

    @Override
    public VersionedKeyValueStore<Bytes, byte[]> get() {
        return new RocksDBVersionedStore(name, metricsScope(), historyRetentionMs, segmentIntervalMs);
    }

    @Override
    public String metricsScope() {
        return "rocksdb";
    }
}

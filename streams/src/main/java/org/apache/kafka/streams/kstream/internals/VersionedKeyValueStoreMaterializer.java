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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Materialized.StoreType;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;
import org.apache.kafka.streams.state.internals.RocksDbVersionedKeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.internals.VersionedKeyValueStoreBuilder;

public class VersionedKeyValueStoreMaterializer<K, V> {
    private final MaterializedInternal<K, V, VersionedKeyValueStore<Bytes, byte[]>> materialized;

    public VersionedKeyValueStoreMaterializer(final MaterializedInternal<K, V, VersionedKeyValueStore<Bytes, byte[]>> materialized) {
        this.materialized = materialized;
    }

    /**
     * @return  StoreBuilder
     */
    public StoreBuilder<VersionedKeyValueStoreInternal<K, V>> materialize() {
        VersionedBytesStoreSupplier supplier = (VersionedBytesStoreSupplier) materialized.storeSupplier();

        if (supplier == null) {
            switch (materialized.storeType()) {
                case IN_MEMORY:
                    throw new UnsupportedOperationException("in memory versioned stores not implemented");
                case ROCKS_DB:
                    supplier = Stores.persistentVersionedKeyValueStore(
                        materialized.storeName(), materialized.retention(), materialized.segmentInterval()); // TODO(note): doesn't feel right that these come in through the materialized but maybe it's fine since windowed stores seem to do the same thing?
                    break;
                default:
                    throw new IllegalStateException("Unknown store type: " + materialized.storeType());
            }
        }

        final StoreBuilder<VersionedKeyValueStoreInternal<K, V>> builder = new VersionedKeyValueStoreBuilder<>(
            supplier,
            materialized.keySerde(),
            materialized.valueSerde(),
            Time.SYSTEM); // TODO: probably not the right place for this. also, does value serde need to be wrapped?

        if (materialized.loggingEnabled()) {
            builder.withLoggingEnabled(materialized.logConfig());
        } else {
            // TODO: do we want to enable disabling logging?
            builder.withLoggingDisabled();
        }

        if (materialized.cachingEnabled()) {
            // TODO: throw since enabling caching is a no-op?
            builder.withCachingEnabled();
        }
        return builder;
    }
}

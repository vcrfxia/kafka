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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.internals.CachingTimeAwareKeyValueStore.CacheableVersionedStoreCallbacks;

public class VersionedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, VersionedKeyValueStore<K, V>> {

    private final RocksDbVersionedKeyValueBytesStoreSupplier storeSupplier;
    private final Serde<V> innerValueSerde;

    public VersionedKeyValueStoreBuilder(final RocksDbVersionedKeyValueBytesStoreSupplier storeSupplier,
                                           final Serde<K> keySerde,
                                           final Serde<V> valueSerde,
                                           final Time time) {
        super(
            storeSupplier.name(),
            keySerde,
            null, // TODO(note): needs to be typed as if new ValueAndTimestampSerde<>(valueSerde) but we don't actually want this to be used, thus the null hack for now. consider updating the type required in AbstractStoreBuilder instead
            time);
        this.innerValueSerde = valueSerde;
        this.storeSupplier = storeSupplier;
    }

    @Override
    public VersionedKeyValueStore<K, V> build() {
        VersionedKeyValueStore<Bytes, byte[]> store = new RocksDBVersionedStore(
            name,
            storeSupplier.metricsScope(),
            storeSupplier.historyRetentionMs(),
            storeSupplier.segmentIntervalMs()
        );

        return new MeteredTimeAwareKeyValueStore<>(
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            innerValueSerde);
    }

    public long historyRetention() {
        return storeSupplier.historyRetentionMs();
    }

    private VersionedKeyValueStore<Bytes, byte[]> maybeWrapCaching(final VersionedKeyValueStore<Bytes, byte[]> inner) {
        if (!enableCaching) {
            return inner;
        }
        if (!(inner instanceof CacheableVersionedKeyValueStore)) {
            throw new IllegalArgumentException("cannot enable caching for a versioned store which does not support it");
        }
        return new CachingTimeAwareKeyValueStore(inner, new CacheableVersionedStoreCallbacks() {
            @Override
            public void replaceFromCache(Bytes key, ValueAndTimestamp<byte[]> value, long nextTimestamp) {
                ((CacheableVersionedKeyValueStore<Bytes, byte[]>) inner).replaceFromCache(key, value, nextTimestamp);
            }

            @Override
            public void bypassCache(Bytes key, ValueAndTimestamp<byte[]> value, long nextTimestamp) {
                ((CacheableVersionedKeyValueStore<Bytes, byte[]>) inner).bypassCache(key, value, nextTimestamp);
            }

            @Override
            public void newKeyInsertedToCache(Bytes key, long nextTimestamp) {
                ((CacheableVersionedKeyValueStore<Bytes, byte[]>) inner).newKeyInsertedToCache(key, nextTimestamp);
            }
        });
    }

    private VersionedKeyValueStore<Bytes, byte[]> maybeWrapLogging(final VersionedKeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return inner instanceof CacheableVersionedKeyValueStore
            ? new CacheableChangeLoggingTimeAwareKeyValueBytesStore((CacheableVersionedKeyValueStore<Bytes, byte[]>) inner)
            : new ChangeLoggingTimeAwareKeyValueBytesStore<>(inner);
    }
}
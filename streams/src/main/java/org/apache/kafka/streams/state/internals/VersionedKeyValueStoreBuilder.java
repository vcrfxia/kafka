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

import org.apache.kafka.streams.state.VersionedBytesStoreSupplier;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;

public class VersionedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, VersionedKeyValueStoreInternal<K, V>> {

    private final VersionedBytesStoreSupplier storeSupplier;
    private final Serde<V> innerValueSerde;

    public VersionedKeyValueStoreBuilder(final VersionedBytesStoreSupplier storeSupplier,
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
    public VersionedKeyValueStoreInternal<K, V> build() {
        // TODO(here): this needs to be updated to actually call storeSupplier.get() and wrap the result in the internal representation
        VersionedKeyValueStoreInternal<Bytes, byte[]> store = new RocksDBVersionedStore(
            name,
            storeSupplier.metricsScope(),
            storeSupplier.historyRetentionMs(),
            storeSupplier.segmentIntervalMs()
        );

        return new MeteredTimeAwareKeyValueStore<>(
            maybeWrapLogging(store), // TODO(note): there used to be a caching layer here but we won't have one for versioned tables
            storeSupplier.metricsScope(),
            time,
            keySerde,
            innerValueSerde);
    }

    public long historyRetention() {
        return storeSupplier.historyRetentionMs();
    }

    private VersionedKeyValueStoreInternal<Bytes, byte[]> maybeWrapLogging(final VersionedKeyValueStoreInternal<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimeAwareKeyValueBytesStore<>(inner);
    }
}
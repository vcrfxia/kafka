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
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import org.apache.kafka.streams.state.VersionedKeyValueStore;

public class VersionedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> {

    private final VersionedKeyValueBytesStoreSupplier storeSupplier;
    private final Serde<V> innerValueSerde;

    public VersionedKeyValueStoreBuilder(final String name,
                                           final Serde<K> keySerde,
                                           final Serde<V> valueSerde,
                                           final Time time) {
        super(
            name,
            keySerde,
            null, // TODO(note): needs to be typed as if new ValueAndTimestampSerde<>(valueSerde) but we don't actually want this to be used, thus the null hack for now. consider updating the type required in AbstractStoreBuilder instead
            time);
        this.innerValueSerde = valueSerde;
        this.storeSupplier = new RocksDBVersionedStoreSupplier(name);
    }

    @Override
    public VersionedKeyValueStore<K, V> build() {
        VersionedKeyValueStore<Bytes, byte[]> store = storeSupplier.get();
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
        return new CachingTimeAwareKeyValueStore(inner);
    }

    private VersionedKeyValueStore<Bytes, byte[]> maybeWrapLogging(final VersionedKeyValueStore<Bytes, byte[]> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimeAwareKeyValueBytesStore(inner);
    }

    // TODO(note): same as KeyValueBytesStoreSupplier except with ValueAndTimestamp already present.
    // also needs additional methods to expose retention period and segment interval (see WindowBytesStoreSupplier for inspiration)
    // TODO: should this be moved to be a public interface, similar to WindowBytesStoreSupplier?
    interface VersionedKeyValueBytesStoreSupplier
        extends StoreSupplier<VersionedKeyValueStore<Bytes, byte[]>> {

        /**
         * The history retention (in milliseconds) for the {@link VersionedKeyValueStore}.
         *
         * @return history retention (in milliseconds)
         */
        long historyRetentionMs();

        /**
         * The size of the segments (in milliseconds) the store has.
         *
         * @return size of the segments (in milliseconds)
         */
        long segmentIntervalMs();
    }

    public static class RocksDBVersionedStoreSupplier implements VersionedKeyValueBytesStoreSupplier {

        private final String name;
        private final long historyRetentionMs;
        private final long segmentIntervalMs;

        public RocksDBVersionedStoreSupplier(final String name) {
            this.name = name;
            // TODO: do not hard code history retention
            this.historyRetentionMs = 300_000L;
            this.segmentIntervalMs = 150_000L;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public long historyRetentionMs() {
            return historyRetentionMs;
        }

        @Override
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
}
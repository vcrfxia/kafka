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
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreSupplier;
import org.apache.kafka.streams.state.TimestampedBytesStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

public class VersionedKeyValueStoreBuilder<K, V>
    extends AbstractStoreBuilder<K, ValueAndTimestamp<V>, TimestampedKeyValueStore<K, V>> {

    private final TimestampedKeyValueBytesStoreSupplier storeSupplier;

    public VersionedKeyValueStoreBuilder(final String name,
                                           final Serde<K> keySerde,
                                           final Serde<V> valueSerde,
                                           final Time time) {
        super(
            name,
            keySerde,
            valueSerde == null ? null : new ValueAndTimestampSerde<>(valueSerde), // TODO: should this wrapping be here? needs to correspond to where this constructor is called
            time);
        this.storeSupplier = new RocksDBVersionedStoreSupplier(name);
    }

    @Override
    public TimestampedKeyValueStore<K, V> build() {
        KeyValueStore<Bytes, ValueAndTimestamp<byte[]>> store = storeSupplier.get();
        return new MeteredKeyValueStore<>( // TODO: here
            maybeWrapCaching(maybeWrapLogging(store)),
            storeSupplier.metricsScope(),
            time,
            keySerde,
            valueSerde);
    }

    private KeyValueStore<Bytes, ValueAndTimestamp<byte[]>> maybeWrapCaching(final KeyValueStore<Bytes, ValueAndTimestamp<byte[]>> inner) {
        if (!enableCaching) {
            return inner;
        }
        return new CachingKeyValueStore(inner, true);
    }

    private KeyValueStore<Bytes, ValueAndTimestamp<byte[]>> maybeWrapLogging(final KeyValueStore<Bytes, ValueAndTimestamp<byte[]>> inner) {
        if (!enableLogging) {
            return inner;
        }
        return new ChangeLoggingTimestampedKeyValueBytesStore(inner);
    }

    // TODO(note): same as KeyValueBytesStoreSupplier except with ValueAndTimestamp already present
    interface TimestampedKeyValueBytesStoreSupplier
        extends StoreSupplier<KeyValueStore<Bytes, ValueAndTimestamp<byte[]>>> {
    }

    public class RocksDBVersionedStoreSupplier implements TimestampedKeyValueBytesStoreSupplier {

        private final String name;

        public RocksDBVersionedStoreSupplier(final String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public KeyValueStore<Bytes, ValueAndTimestamp<byte[]>> get() {
            // TODO: do not hard code history retention
            return new RocksDBVersionedStore(name, metricsScope(), 300_000L, 150_000L);
        }

        @Override
        public String metricsScope() {
            return "rocksdb";
        }
    }
}
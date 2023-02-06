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

import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

import java.util.Objects;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;

public class MeteredVersionedKeyValueStore<K, V>
    extends MeteredKeyValueStore<K, ValueAndTimestamp<V>>
    implements VersionedKeyValueStoreInternal<K, V> {

    private final VersionedBytesStore inner;

    MeteredVersionedKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                                  final String metricScope,
                                  final Time time,
                                  final Serde<K> keySerde,
                                  final Serde<ValueAndTimestamp<V>> valueSerde) {
        super(inner, metricScope, time, keySerde, valueSerde);
        if (!(inner instanceof VersionedBytesStore)) {
            throw new IllegalStateException("inner store must be versioned");
        }
        this.inner = (VersionedBytesStore) inner;
    }

    @Override
    public void put(final K key, final ValueAndTimestamp<V> value) {
        super.put(
            key,
            value == null // versioned stores require a timestamp associated with all puts, including tombstones/deletes
                ? ValueAndTimestamp.makeAllowNullable(null, context.timestamp())
                : value
        );
    }

    @Override
    public ValueAndTimestamp<V> get(K key, long timestampTo) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            // TODO(note): sharing the existing sensor here is a design choice. think it's better than introducing a new one
            return maybeMeasureLatency(() -> outerValue(inner.get(keyBytes(key), timestampTo)), time, getSensor); // TODO: verify
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public ValueAndTimestamp<V> delete(K key, long timestamp) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            return maybeMeasureLatency(() -> outerValue(inner.delete(keyBytes(key), timestamp)), time, deleteSensor); // TODO: verify
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        // TODO: do we need an override here?
        throw new UnsupportedOperationException("todo");
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Serde<ValueAndTimestamp<V>> prepareValueSerdeForStore(
        final Serde<ValueAndTimestamp<V>> valueSerde,
        final SerdeGetter getter
    ) {
        if (valueSerde == null) {
            return new NullableValueAndTimestampSerde<>((Serde<V>) getter.valueSerde());
        } else {
            return super.prepareValueSerdeForStore(valueSerde, getter);
        }
    }
}

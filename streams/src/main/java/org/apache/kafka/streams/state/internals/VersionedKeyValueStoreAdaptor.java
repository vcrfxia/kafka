package org.apache.kafka.streams.state.internals;

import java.util.Objects;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;
import org.apache.kafka.streams.state.VersionedRecord;

class VersionedKeyValueStoreAdaptor<K, V> implements VersionedKeyValueStore<K, V> {

    private final VersionedKeyValueStoreInternal<K, V> inner;

    VersionedKeyValueStoreAdaptor(final VersionedKeyValueStoreInternal<K, V> inner) {
        this.inner = Objects.requireNonNull(inner);
    }

    @Override
    public void put(K key, V value, long timestamp) {
        inner.put(key, ValueAndTimestamp.makeAllowNullable(value, timestamp));
    }

    @Override
    public VersionedRecord<V> delete(K key, long timestamp) {
        ValueAndTimestamp<V> valueAndTimestamp = inner.get(key, timestamp);
        inner.put(
            key,
            ValueAndTimestamp.makeAllowNullable(null, timestamp)
        );
        return adaptValueAndTimestamp(valueAndTimestamp);
    }

    @Override
    public VersionedRecord<V> get(K key) {
        return adaptValueAndTimestamp(inner.get(key));
    }

    @Override
    public VersionedRecord<V> get(K key, long asOfTimestamp) {
        return adaptValueAndTimestamp(inner.get(key, asOfTimestamp));
    }

    private static <V> VersionedRecord<V> adaptValueAndTimestamp(ValueAndTimestamp<V> valueAndTimestamp) {
        if (valueAndTimestamp == null) {
            return null;
        }
        return VersionedRecord.make(valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    // --- bunch of pass-through methods ---

    @Override
    public String name() {
        return inner.name();
    }

    @Deprecated
    @Override
    public void init(ProcessorContext context, StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void flush() {
        inner.flush();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return inner.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return inner.getPosition();
    }
}

package org.apache.kafka.streams.state.internals;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueTimestampIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;

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
    public ValueAndTimestamp<V> get(K key) {
        return inner.get(key);
    }

    @Override
    public ValueAndTimestamp<V> get(K key, long timestampTo) {
        return inner.get(key, timestampTo);
    }

    // --- bunch of other methods from VersionedKeyValueStore, which are adapted similarly (implementations left out for brevity) ---

    @Override
    public ValueAndTimestamp<V> putIfAbsent(K key, V value, long timestamp) {
        return null;
    }

    @Override
    public void putAll(List<KeyValueTimestamp<K, V>> entries) {

    }

    @Override
    public V delete(K key) {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> range(K from, K to) {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> range(K from, K to, long timestampTo) {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> reverseRange(K from, K to) {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> reverseRange(K from, K to, long timestampTo) {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> all() {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> all(long timestampTo) {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> reverseAll() {
        return null;
    }

    @Override
    public KeyValueTimestampIterator<K, V> reverseAll(long timestampTo) {
        return null;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueTimestampIterator<K, byte[]> prefixScan(P prefix, PS prefixKeySerializer) {
        return null;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueTimestampIterator<K, byte[]> prefixScan(P prefix, PS prefixKeySerializer, long timestampTo) {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {

    }

    @Override
    public void init(StateStoreContext context, StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return null;
    }

    @Override
    public Position getPosition() {
        return null;
    }
}

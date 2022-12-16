package org.apache.kafka.streams.state.internals;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;

/**
 * Adapts from VersionedKeyValueStore (user-friendly versioned store interface) to KeyValueStore,
 * in order to unify the two (and therefore share existing StreamsBuilder and KTable method interfaces
 * which accept Materialized<K, V, KeyValueStore<Bytes, byte[]>).
 */
public class VersionedBytesStoreAdaptor implements VersionedBytesStore {

    final VersionedKeyValueStore<Bytes, byte[]> inner;

    public VersionedBytesStoreAdaptor(final VersionedKeyValueStore<Bytes, byte[]> inner) {
        this.inner = Objects.requireNonNull(inner);
    }

    @Override
    public void put(Bytes key, byte[] valueAndTimestamp) {
        final boolean isTombstone = VersionedBytesStoreValueFormatter.isTombstone(valueAndTimestamp);
        inner.put(
            key,
            isTombstone ? null : VersionedBytesStoreValueFormatter.rawValue(valueAndTimestamp),
            VersionedBytesStoreValueFormatter.timestamp(valueAndTimestamp)
        );
    }

    // returns timestamp, bool, and value
    @Override
    public byte[] get(Bytes key) {
        final VersionedRecord<byte[]> versionedRecord = inner.get(key);
        return VersionedBytesStoreValueFormatter.toReturnBytes(versionedRecord);
    }

    // returns timestamp, bool, and value
    @Override
    public byte[] get(Bytes key, long timestampTo) {
        final VersionedRecord<byte[]> versionedRecord = inner.get(key, timestampTo);
        return VersionedBytesStoreValueFormatter.toReturnBytes(versionedRecord);
    }

    // returns timestamp, bool, and value
    @Override
    public byte[] delete(Bytes key, long timestamp) {
        final VersionedRecord<byte[]> versionedRecord = inner.get(key, timestamp);
        inner.put(key, null, timestamp);
        return VersionedBytesStoreValueFormatter.toReturnBytes(versionedRecord);
    }

    // --- bunch of methods which are direct pass-throughs ---

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
        return inner.persistent();
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return inner.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return inner.getPosition();
    }

    // --- bunch of methods that VersionedKeyValueStore does not support ---

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support putIfAbsent(key, value)");
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support putAll(entries)");
    }

    @Override
    public byte[] delete(Bytes key) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support delete(key)");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support range(from, to)");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support reverseRange(from, to)");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        throw new UnsupportedOperationException("Versioned key-value stores do not support all()");
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        throw new UnsupportedOperationException("Versioned key-value stores do not support reverseAll()");
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(P prefix, PS prefixKeySerializer) {
        throw new UnsupportedOperationException("Versioned key-value stores do not support prefixScan(prefix, prefixKeySerializer)");
    }

    @Override
    public long approximateNumEntries() {
        throw new UnsupportedOperationException("Versioned key-value stores do not support approximateNumEntries()");
    }

}

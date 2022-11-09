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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;

/**
 * Adapts from VersionedBytesStore to VersionedKeyValueStoreInternal, to be used throughout
 * the codebase everywhere that TimestampedKeyValueStore is used today.
 */
class VersionedKeyValueStoreAdaptor implements VersionedKeyValueStoreInternal<Bytes, byte[]> {

    private final VersionedBytesStore inner;

    VersionedKeyValueStoreAdaptor(final VersionedBytesStore inner) {
        this.inner = Objects.requireNonNull(inner);
    }

    @Override
    public void put(Bytes key, ValueAndTimestamp<byte[]> valueAndTimestamp) {
        inner.put(key, VersionedBytesStoreValueFormatter.toPutBytes(valueAndTimestamp));
    }

    @Override
    public ValueAndTimestamp<byte[]> get(Bytes key) {
        final byte[] valueAndTimestamp = inner.get(key);
        if (valueAndTimestamp == null) {
            return null;
        }
        return ValueAndTimestamp.make(
            VersionedBytesStoreValueFormatter.rawValue(valueAndTimestamp),
            VersionedBytesStoreValueFormatter.timestamp(valueAndTimestamp)
        );
    }

    @Override
    public ValueAndTimestamp<byte[]> get(Bytes key, long timestampTo) {
        final byte[] valueAndTimestamp = inner.get(key, timestampTo);
        if (valueAndTimestamp == null) {
            return null;
        }
        return ValueAndTimestamp.make(
            VersionedBytesStoreValueFormatter.rawValue(valueAndTimestamp),
            VersionedBytesStoreValueFormatter.timestamp(valueAndTimestamp)
        );
    }


    // --- bunch of other methods from TimestampedKeyValueStore, which are adapted similarly (implementations left out for brevity) ---

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(Bytes from, Bytes to, long timestampTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(Bytes from, Bytes to, long timestampTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all(long timestampTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll(long timestampTo) {
        return null;
    }

    @Override
    public ValueAndTimestamp<byte[]> putIfAbsent(Bytes key, ValueAndTimestamp<byte[]> value) {
        return null;
    }

    @Override
    public void putAll(List<KeyValue<Bytes, ValueAndTimestamp<byte[]>>> entries) {

    }

    @Override
    public ValueAndTimestamp<byte[]> delete(Bytes key) {
        return null;
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

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(Bytes from, Bytes to) {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(Bytes from, Bytes to) {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all() {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll() {
        return null;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> prefixScan(P prefix, PS prefixKeySerializer) {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0;
    }
}

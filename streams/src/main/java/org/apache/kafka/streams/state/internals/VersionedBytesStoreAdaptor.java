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
import org.apache.kafka.streams.state.VersionedKeyValueStore;

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
        final ValueAndTimestamp<byte[]> valueAndTimestamp = inner.get(key);
        return VersionedBytesStoreValueFormatter.toReturnBytes(valueAndTimestamp);
    }

    // returns timestamp, bool, and value
    @Override
    public byte[] get(Bytes key, long timestampTo) {
        final ValueAndTimestamp<byte[]> valueAndTimestamp = inner.get(key, timestampTo);
        return VersionedBytesStoreValueFormatter.toReturnBytes(valueAndTimestamp);
    }


    // --- bunch of other methods from KeyValueStore, which are adapted similarly (implementations left out for brevity) ---

    @Override
    public byte[] putIfAbsent(Bytes key, byte[] value) {
        return new byte[0];
    }

    @Override
    public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
    }

    @Override
    public byte[] delete(Bytes key) {
        return new byte[0];
    }

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
        return null;
    }

    @Override
    public Position getPosition() {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(Bytes from, Bytes to) {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        return null;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, byte[]> prefixScan(P prefix, PS prefixKeySerializer) {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0;
    }

}

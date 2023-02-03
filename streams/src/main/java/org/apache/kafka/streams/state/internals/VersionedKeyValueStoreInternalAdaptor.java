package org.apache.kafka.streams.state.internals;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
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
class VersionedKeyValueStoreInternalAdaptor implements VersionedKeyValueStoreInternal<Bytes, byte[]> {
    private static final Serde<ValueAndTimestamp<byte[]>> VALUE_AND_TIMESTAMP_SERDE
        = new NullableValueAndTimestampSerde<>(new ByteArraySerde());
    private static final Serializer<ValueAndTimestamp<byte[]>> VALUE_AND_TIMESTAMP_SERIALIZER
        = VALUE_AND_TIMESTAMP_SERDE.serializer();
    private static final Deserializer<ValueAndTimestamp<byte[]>> VALUE_AND_TIMESTAMP_DESERIALIZER
        = VALUE_AND_TIMESTAMP_SERDE.deserializer();

    private final VersionedBytesStore inner;

    VersionedKeyValueStoreInternalAdaptor(final VersionedBytesStore inner) {
        this.inner = Objects.requireNonNull(inner);
    }

    @Override
    public void put(Bytes key, ValueAndTimestamp<byte[]> valueAndTimestamp) {
        final byte[] rawValueAndTimestamp = VALUE_AND_TIMESTAMP_SERIALIZER.serialize(null, valueAndTimestamp);
        inner.put(key, rawValueAndTimestamp);
    }

    @Override
    public ValueAndTimestamp<byte[]> get(Bytes key) {
        final byte[] rawValueAndTimestamp = inner.get(key);
        return VALUE_AND_TIMESTAMP_DESERIALIZER.deserialize(null, rawValueAndTimestamp);
        // TODO: does this need a check to ensure we never return non-null result with null value?
        // previous code was confident that value never comes out as null, not sure how
    }

    @Override
    public ValueAndTimestamp<byte[]> get(Bytes key, long timestampTo) {
        final byte[] rawValueAndTimestamp = inner.get(key, timestampTo);
        return VALUE_AND_TIMESTAMP_DESERIALIZER.deserialize(null, rawValueAndTimestamp);
        // TODO: does this need a check to ensure we never return non-null result with null value?
        // previous code was confident that value never comes out as null, not sure how
    }


    // --- bunch of other methods from TimestampedKeyValueStore, which are adapted similarly (implementations left out for brevity) ---

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

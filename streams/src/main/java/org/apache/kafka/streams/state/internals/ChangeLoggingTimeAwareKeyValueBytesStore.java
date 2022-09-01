package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;

import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

public class ChangeLoggingTimeAwareKeyValueBytesStore
        extends WrappedStateStore<VersionedKeyValueStore<Bytes, byte[]>, byte[], ValueAndTimestamp<byte[]>>
        implements VersionedKeyValueStore<Bytes, byte[]> {

    InternalProcessorContext context;

    ChangeLoggingTimeAwareKeyValueBytesStore(final VersionedKeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
        // TODO: this used to set an eviction listener. is that still needed? (has to do with caches, revisit when implementing cache)
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        this.context = asInternalProcessorContext(context);
        super.init(context, root);
        // TODO: this used to set an eviction listener. is that still needed? (has to do with caches, revisit when implementing cache)
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    @Override
    public void put(final Bytes key,
                    final ValueAndTimestamp<byte[]> value) {
        wrapped().put(key, value);
        log(key, value);
    }

    @Override
    public ValueAndTimestamp<byte[]> putIfAbsent(final Bytes key,
                              final ValueAndTimestamp<byte[]> value) {
        final ValueAndTimestamp<byte[]> previous = wrapped().putIfAbsent(key, value);
        if (previous == null) {
            // then it was absent
            log(key, value);
        }
        return previous;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, ValueAndTimestamp<byte[]>>> entries) {
        wrapped().putAll(entries);
        for (final KeyValue<Bytes, ValueAndTimestamp<byte[]>> entry : entries) {
            log(entry.key, entry.value);
        }
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> prefixScan(final P prefix,
                                                                                    final PS prefixKeySerializer) {
        return wrapped().prefixScan(prefix, prefixKeySerializer);
    }

    @Override
    public ValueAndTimestamp<byte[]> delete(final Bytes key) {
        final ValueAndTimestamp<byte[]> oldValue = wrapped().delete(key);
        log(key, null);
        return oldValue;
    }

    @Override
    public ValueAndTimestamp<byte[]> get(final Bytes key) {
        return wrapped().get(key);
    }

    @Override
    public ValueAndTimestamp<byte[]> get(final Bytes key, final long timestampTo) {
        return wrapped().get(key, timestampTo);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(final Bytes from,
                                                 final Bytes to) {
        return wrapped().range(from, to);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(
        final Bytes from, final Bytes to, final long timestampTo) {
        return wrapped().range(from, to, timestampTo);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(final Bytes from,
                                                        final Bytes to) {
        return wrapped().reverseRange(from, to);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(
        final Bytes from, final Bytes to, final long timestampTo) {
        return wrapped().reverseRange(from, to, timestampTo);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all() {
        return wrapped().all();
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all(final long timestampTo) {
        return wrapped().all(timestampTo);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll() {
        return wrapped().reverseAll();
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll(final long timestampTo) {
        return wrapped().reverseAll(timestampTo);
    }

    @Override
    public void deleteHistory(final long timestampTo) {
        wrapped().deleteHistory(timestampTo);
    }

    void log(final Bytes key, final ValueAndTimestamp<byte[]> value) {
        // TODO(note): this logic stays pretty much the same but need to figure out where to change configs for the topic
        context.logChange(
            name(),
            key,
            value == null ? null : value.value(), // TODO(note): should only actually come in as null from delete()
            context.timestamp(), // TODO: make consistent with regards to where the timestamp comes from
            wrapped().getPosition()
        );
    }
}

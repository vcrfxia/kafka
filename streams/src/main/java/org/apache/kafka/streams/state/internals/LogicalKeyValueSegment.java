package org.apache.kafka.streams.state.internals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogicalKeyValueSegment implements Comparable<LogicalKeyValueSegment>, Segment {
    private static final Logger log = LoggerFactory.getLogger(LogicalKeyValueSegment.class);

    public final long id;
    private final String name;
    private final RocksDBStore physicalStore;
    private final PrefixLogicalFormatter prefixLogicalFormatter;

    private volatile boolean open = false;
    // open iterators are tracked here rather than in the underlying rocksdbstore (rocksdbstore needs refactoring in final implementation)
    final Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections.synchronizedSet(new HashSet<>());

    LogicalKeyValueSegment(final long id, final String name, final RocksDBStore physicalStore) {
        this.id = id;
        this.name = name;
        this.physicalStore = Objects.requireNonNull(physicalStore);

        this.prefixLogicalFormatter = new PrefixLogicalFormatter(serializeLongToBytes(id));
    }

    void openDB() {
        open = true;
    }

    @Override
    public int compareTo(final LogicalKeyValueSegment segment) {
        return Long.compare(id, segment.id);
    }

    @Override
    public void destroy() throws IOException {
        // TODO(note): this usage is weird but pretty sure it's correct:
        // deleteRange() calls Bytes.increment() in order to make keyTo inclusive,
        // so passing the same value for keyFrom and keyTo turns this into a prefix deletion
        // which is exactly what we want
        physicalStore.deleteRange(
            prefixLogicalFormatter.getKeyPrefixForUnderlyingStore(),
            prefixLogicalFormatter.getKeyPrefixForUnderlyingStore());

        // TODO(note): KeyValueSegment implementation of this method involves deleting
        // the dir for the segment as a form of cleanup. not needed in this case since we
        // want to keep the dir open (same dir used for all the logicals)
    }

    @Override
    public void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        physicalStore.deleteRange(
            prefixLogicalFormatter.translateKeyForUnderlyingStore(keyFrom),
            prefixLogicalFormatter.translateKeyForUnderlyingStore(keyTo));
    }

    @Override
    public void put(final Bytes key, final byte[] value) {
        put(key, value, false);
    }
    public void put(final Bytes key, final byte[] value, final boolean isRestoring) {
        physicalStore.put(
            prefixLogicalFormatter.translateKeyForUnderlyingStore(key),
            value,
            isRestoring);
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        return physicalStore.putIfAbsent(
            prefixLogicalFormatter.translateKeyForUnderlyingStore(key),
            value);
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        physicalStore.putAll(entries.stream()
            .map(kv -> new KeyValue<>(
                prefixLogicalFormatter.translateKeyForUnderlyingStore(kv.key),
                kv.value))
            .collect(Collectors.toList()));
    }

    @Override
    public byte[] delete(final Bytes key) {
        return physicalStore.delete(prefixLogicalFormatter.translateKeyForUnderlyingStore(key));
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        // for segments, init() shouldn't be called. equivalent path is instead openExisting()
        // on the Segments, followed by getOrCreateSegment() in order to initialize
        // the logical segment itself. (RocksDBStore has both pathways because it can
        // also be used as its own standalone store separate from segments usage)
        throw new IllegalStateException("init should not be called on logical segment");
    }

    @Override
    public void flush() {
        throw new IllegalStateException("flush should not be called on logical segment");
    }

    // should still be called on logicals even if the physical is also being closed
    @Override
    public synchronized void close() {
        if (!open) {
            return;
        }

        open = false;
        closeOpenIterators();
    }

    private void closeOpenIterators() {
        final HashSet<KeyValueIterator<Bytes, byte[]>> iterators;
        synchronized (openIterators) {
            iterators = new HashSet<>(openIterators);
        }
        if (iterators.size() != 0) {
            log.warn("Closing {} open iterators for store {}", iterators.size(), name);
            for (final KeyValueIterator<Bytes, byte[]> iterator : iterators) {
                iterator.close();
            }
        }
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public byte[] get(final Bytes key) {
        return get(key, false);
    }
    public byte[] get(final Bytes key, final boolean isRestoring) {
        return physicalStore.get(prefixLogicalFormatter.translateKeyForUnderlyingStore(key), isRestoring);
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        // TODO(note): logical tracks iterators, not the physical
        final KeyValueIterator<Bytes, byte[]> rocksDBRangeIterator = physicalStore.range(prefixLogicalFormatter.translateKeyForUnderlyingStore(from), prefixLogicalFormatter.translateKeyForUnderlyingStore(to), openIterators);
        openIterators.add(rocksDBRangeIterator);

        return rocksDBRangeIterator;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        // TODO(note): logical tracks iterators, not the physical
        final KeyValueIterator<Bytes, byte[]> rocksDbPrefixSeekIterator = physicalStore.prefixScan(prefixLogicalFormatter.getKeyPrefixForUnderlyingStore(), new BytesSerializer(), openIterators);
        openIterators.add(rocksDbPrefixSeekIterator);

        return rocksDbPrefixSeekIterator;
    }

    @Override
    public long approximateNumEntries() {
        // rocksdb metric that normally tracks this doesn't work for ranges
        throw new UnsupportedOperationException();
    }

    @Override
    public void addToBatch(final KeyValue<byte[], byte[]> record, final WriteBatch batch) throws RocksDBException {
        physicalStore.addToBatch(new KeyValue<>(prefixLogicalFormatter.translateKeyForUnderlyingStore(record.key), record.value), batch);
    }

    @Override
    public void write(final WriteBatch batch) throws RocksDBException {
        // no key transformations here since assumption is that they've already been done
        physicalStore.write(batch);
    }

    // TODO: create interface
    private static class PrefixLogicalFormatter {
        private final byte[] prefix;

        PrefixLogicalFormatter(final Bytes prefix) {
            this.prefix = prefix.get();
        }

        Bytes translateKeyForUnderlyingStore(final Bytes key) {
            return Bytes.wrap(translateKeyForUnderlyingStore(key.get()));
        }

        byte[] translateKeyForUnderlyingStore(final byte[] key) {
            final byte[] res = new byte[prefix.length + key.length];
            System.arraycopy(prefix, 0, res, 0, prefix.length);
            System.arraycopy(key, 0, res, prefix.length, key.length);
            return res;
        }

        Bytes getKeyPrefixForUnderlyingStore() {
            return Bytes.wrap(prefix);
        }
    }

    private static Bytes serializeLongToBytes(final long l) {
        return Bytes.wrap(ByteBuffer.allocate(Long.BYTES).putLong(l).array());
    }
}

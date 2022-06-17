package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.executeAll;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.throwSuppressed;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorRecordContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: de-dup from CachingKeyValueStore
public class CachingTimeAwareKeyValueStore
    extends WrappedStateStore<TimestampedKeyValueStore<Bytes, byte[]>, byte[], ValueAndTimestamp<byte[]>>
    implements TimestampedKeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], ValueAndTimestamp<byte[]>> {
    // TODO: why does CachedStateStore use byte[] for key type, instead of Bytes?
    // TODO: is it preferable to create a new interface analogous to CachedStateStore instead?

    private static final Logger LOG = LoggerFactory.getLogger(CachingTimeAwareKeyValueStore.class);

    private CacheFlushListener<byte[], ValueAndTimestamp<byte[]>> flushListener;
    private boolean sendOldValues;
    private String cacheName;
    private InternalProcessorContext<?, ?> context;
    private Thread streamThread;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Position position;

    CachingTimeAwareKeyValueStore(final TimestampedKeyValueStore<Bytes, byte[]> inner) {
        super(inner);
        position = Position.emptyPosition();
    }

    @SuppressWarnings("deprecation") // This can be removed when it's removed from the interface.
    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        initInternal(asInternalProcessorContext(context));
        super.init(context, root);
        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the current thread.
        streamThread = Thread.currentThread();
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        initInternal(asInternalProcessorContext(context));
        super.init(context, root);
        // save the stream thread as we only ever want to trigger a flush
        // when the stream thread is the current thread.
        streamThread = Thread.currentThread();
    }

    @Override
    public Position getPosition() {
        // TODO: what does this mean?
        // We return the merged position since the query uses the merged position as well
        final Position mergedPosition = Position.emptyPosition();
        mergedPosition.merge(position);
        mergedPosition.merge(wrapped().getPosition());
        return mergedPosition;
    }

    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {
        return null; // TODO
    }

    private void initInternal(final InternalProcessorContext<?, ?> context) {
        this.context = context;
        this.cacheName = ThreadCache.nameSpaceFromTaskIdAndStore(context.taskId().toString(), name());
        this.context.registerCacheFlushListener(cacheName, entries -> {
            for (final ThreadCache.DirtyEntry entry : entries) {
                putAndMaybeForward(entry, context);
            }
        });
    }

    private void putAndMaybeForward(final ThreadCache.DirtyEntry entry,
                                    final InternalProcessorContext<?, ?> context) {
        if (flushListener != null) {
            final ValueAndTimestamp<byte[]> rawNewValue = entry.newTimeAwareValue(); // TODO(note): requires cache to populate time aware version instead
            final boolean rawNewValueIsNull = rawNewValue == null || rawNewValue.value() == null;
            final ValueAndTimestamp<byte[]> rawOldValue = rawNewValueIsNull || sendOldValues ? wrapped().get(entry.key()) : null;
            final boolean rawOldValueIsNull = rawOldValue == null || rawOldValue.value() == null;

            // this is an optimization: if this key did not exist in underlying store and also not in the cache,
            // we can skip flushing to downstream as well as writing to underlying store
            if (!rawNewValueIsNull || !rawOldValueIsNull) { // TODO: check this logic
                // we need to get the old values if needed, and then put to store, and then flush
                final ProcessorRecordContext current = context.recordContext();
                try {
                    context.setRecordContext(entry.entry().context());
                    wrapped().put(entry.key(), entry.newTimeAwareValue());
                    flushListener.apply(
                        new Record<>(
                            entry.key().get(),
                            new Change<>(rawNewValue, sendOldValues ? rawOldValue : null),
                            entry.entry().context().timestamp(),
                            entry.entry().context().headers()));
                } finally {
                    context.setRecordContext(current);
                }
            }
        } else {
            final ProcessorRecordContext current = context.recordContext();
            try {
                context.setRecordContext(entry.entry().context());
                wrapped().put(entry.key(), entry.newTimeAwareValue());
            } finally {
                context.setRecordContext(current);
            }
        }
    }

    @Override
    public boolean setFlushListener(final CacheFlushListener<byte[], ValueAndTimestamp<byte[]>> flushListener,
                                    final boolean sendOldValues) {
        this.flushListener = flushListener;
        this.sendOldValues = sendOldValues;

        return true;
    }

    @Override
    public void put(final Bytes key,
                    final ValueAndTimestamp<byte[]> value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            // for null bytes, we still put it into cache indicating tombstones
            putInternal(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void putInternal(final Bytes key,
                             final ValueAndTimestamp<byte[]> value) {
        context.cache().put( // TODO: update cache logic
            cacheName,
            key,
            new LRUCacheEntry(
                value.value(),
                context.headers(),
                true,
                context.offset(),
                context.timestamp(), // TODO: here's another case of store fetching timestamp from context. maybe this really is valid, and that my additional changes are unnecessary?
                context.partition(),
                context.topic()));

        StoreQueryUtils.updatePosition(position, context);
    }

    @Override
    public ValueAndTimestamp<byte[]> putIfAbsent(final Bytes key,
                              final ValueAndTimestamp<byte[]> value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            final ValueAndTimestamp<byte[]> v = getInternal(key);
            if (v == null) {
                putInternal(key, value);
            }
            return v;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, ValueAndTimestamp<byte[]>>> entries) {
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            for (final KeyValue<Bytes, ValueAndTimestamp<byte[]>> entry : entries) {
                Objects.requireNonNull(entry.key, "key cannot be null");
                put(entry.key, entry.value);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public ValueAndTimestamp<byte[]> delete(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            return deleteInternal(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private ValueAndTimestamp<byte[]> deleteInternal(final Bytes key) {
        final ValueAndTimestamp<byte[]> v = getInternal(key);
        putInternal(key, null);
        return v;
    }

    @Override
    public ValueAndTimestamp<byte[]> get(final Bytes key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();
        final Lock theLock;
        if (Thread.currentThread().equals(streamThread)) {
            theLock = lock.writeLock();
        } else {
            theLock = lock.readLock();
        }
        theLock.lock();
        try {
            validateStoreOpen();
            return getInternal(key);
        } finally {
            theLock.unlock();
        }
    }

    private ValueAndTimestamp<byte[]> getInternal(final Bytes key) {
        LRUCacheEntry entry = null;
        if (context.cache() != null) {
            entry = context.cache().get(cacheName, key);
        }
        if (entry == null) {
            final ValueAndTimestamp<byte[]> rawValue = wrapped().get(key);
            if (rawValue == null) {
                return null;
            }
            // only update the cache if this call is on the streamThread
            // as we don't want other threads to trigger an eviction/flush
            if (Thread.currentThread().equals(streamThread)) {
                context.cache().put(cacheName, key, new LRUCacheEntry(rawValue.value(), new RecordHeaders(), false, -1, rawValue.timestamp(), -1, "")); // TODO(note): hack to preserve timestamp
            }
            return rawValue;
        } else {
            return ValueAndTimestamp.make(entry.value(), entry.context().timestamp());
        }
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(final Bytes from,
                                                 final Bytes to) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator = wrapped().range(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().range(cacheName, from, to);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(final Bytes from,
                                                        final Bytes to) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator = wrapped().reverseRange(from, to);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().reverseRange(cacheName, from, to);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, false);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all() {
        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator =
            new DelegatingPeekingKeyValueIterator<>(this.name(), wrapped().all());
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().all(cacheName);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> prefixScan(final P prefix, final PS prefixKeySerializer) {
        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator = wrapped().prefixScan(prefix, prefixKeySerializer);
        final Bytes from = Bytes.wrap(prefixKeySerializer.serialize(null, prefix));
        final Bytes to = Bytes.increment(from);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().range(cacheName, from, to, false);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, true);
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll() {
        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator =
            new DelegatingPeekingKeyValueIterator<>(this.name(), wrapped().reverseAll());
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().reverseAll(cacheName);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(cacheIterator, storeIterator, false);
    }

    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        lock.readLock().lock();
        try {
            validateStoreOpen();
            return wrapped().approximateNumEntries();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void flush() {
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            context.cache().flush(cacheName);
            wrapped().flush();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void flushCache() {
        validateStoreOpen();
        lock.writeLock().lock();
        try {
            validateStoreOpen();
            context.cache().flush(cacheName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            final LinkedList<RuntimeException> suppressed = executeAll(
                () -> context.cache().flush(cacheName),
                () -> context.cache().close(cacheName),
                wrapped()::close
            );
            if (!suppressed.isEmpty()) {
                throwSuppressed("Caught an exception while closing caching key value store for store " + name(),
                    suppressed);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}

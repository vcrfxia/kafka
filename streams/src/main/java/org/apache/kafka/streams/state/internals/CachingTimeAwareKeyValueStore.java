package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.executeAll;
import static org.apache.kafka.streams.state.internals.ExceptionUtils.throwSuppressed;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
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
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: de-dup from CachingKeyValueStore
public class CachingTimeAwareKeyValueStore
    extends WrappedStateStore<VersionedKeyValueStore<Bytes, byte[]>, byte[], ValueAndTimestamp<byte[]>>
    implements VersionedKeyValueStore<Bytes, byte[]>, CachedStateStore<byte[], ValueAndTimestamp<byte[]>> {
    // TODO: why does CachedStateStore use byte[] for key type, instead of Bytes?
    // TODO: is it preferable to create a new interface analogous to CachedStateStore instead?

    private static final Logger LOG = LoggerFactory.getLogger(CachingTimeAwareKeyValueStore.class);

    private final CacheableVersionedStoreCallbacks storeCallbacks;
    private CacheFlushListener<byte[], ValueAndTimestamp<byte[]>> flushListener;
    private boolean sendOldValues;
    private String cacheName;
    private InternalProcessorContext<?, ?> context;
    private Thread streamThread;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Position position;

    interface CacheableVersionedStoreCallbacks {
        void replaceFromCache(Bytes key, ValueAndTimestamp<byte[]> value, long nextTimestamp);
        void bypassCache(Bytes key, ValueAndTimestamp<byte[]> value, long nextTimestamp);
        void newKeyInsertedToCache(Bytes key, long nextTimestamp);
    }

    CachingTimeAwareKeyValueStore(
        final VersionedKeyValueStore<Bytes, byte[]> inner,
        final CacheableVersionedStoreCallbacks storeCallbacks
    ) {
        super(inner);
        this.storeCallbacks = storeCallbacks;
        this.position = Position.emptyPosition();
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
        final ValueAndTimestamp<byte[]> newValue = ValueAndTimestamp.makeAllowNullable(
            entry.newValue(),
            entry.entry().context().timestamp()
        );
        if (flushListener != null) {
            final boolean newValueIsNull = newValue.value() == null;
            final ValueAndTimestamp<byte[]> oldValue = newValueIsNull || sendOldValues ? wrapped().get(entry.key()) : null;
            final boolean oldValueIsNull = oldValue == null || oldValue.value() == null;

            // we need to get the old values if needed, and then put to store, and then flush
            final ProcessorRecordContext current = context.recordContext();
            try {
                context.setRecordContext(entry.entry().context());
                // versioned stores require that we always write to underlying store
                wrapped().put(entry.key(), newValue);
                // this is an optimization: if this key did not exist in underlying store and also not in the cache,
                // we can skip flushing to downstream.
                if (!newValueIsNull || !oldValueIsNull) {
                    flushListener.apply(
                        new Record<>(
                            entry.key().get(),
                            new Change<>(newValue, sendOldValues ? oldValue : null),
                            entry.entry().context().timestamp(),
                            entry.entry().context().headers()));
                }
            } finally {
                context.setRecordContext(current);
            }
        } else {
            final ProcessorRecordContext current = context.recordContext();
            try {
                context.setRecordContext(entry.entry().context());
                wrapped().put(entry.key(), newValue);
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
        if (value.timestamp() != context.timestamp()) { // TODO: remove
            throw new IllegalStateException("value and context timestamps do not match");
        }

        // get from cache to check whether this put() is the latest
        LRUCacheEntry oldEntry = null;
        if (context.cache() != null) {
            oldEntry = context.cache().get(cacheName, key);
        }
        if (oldEntry != null) {
            // cache is assumed to contain the latest value
            if (oldEntry.isDirty()) {
                if (value.timestamp() > oldEntry.context().timestamp()) {
                    // insert into cache and flush old entry
                    putToCache(key, value, true);
                    storeCallbacks.replaceFromCache(key, ValueAndTimestamp.makeAllowNullable(oldEntry.value(), oldEntry.context().timestamp()), value.timestamp()); // i.e., put to store bypassing latest value
                } else if (value.timestamp() < oldEntry.context().timestamp()) {
                    // insert into underlying store
                    storeCallbacks.bypassCache(key, value, oldEntry.context().timestamp()); // i.e., put to store bypassing latest value
                } else {
                    // update cache without affecting underlying store
                    putToCache(key, value, true);
                }
            } else {
                if (value.timestamp() > oldEntry.context().timestamp()) {
                    // insert into cache and instruct underlying store to move latest value
                    putToCache(key, value, true);
                    storeCallbacks.newKeyInsertedToCache(key, value.timestamp()); // i.e., move latest value into segment
                } else if (value.timestamp() < oldEntry.context().timestamp()) {
                    // insert into underlying store
                    storeCallbacks.bypassCache(key, value, oldEntry.context().timestamp()); // i.e., put to store bypassing latest value
                } else {
                    // insert into underlying store and update cache as clean
                    putToCache(key, value, false);
                    wrapped().put(key, value);
                }
            }
        } else {
            // nothing in cache. get from underlying store instead
            final ValueAndTimestamp<byte[]> oldValue = wrapped().get(key); // TODO: would like to distinguish between null and null wrapped with timestamp if possible
            if (oldValue != null) {
                if (value.timestamp() > oldValue.timestamp()) {
                    // insert into cache and instruct underlying store to move latest value
                    putToCache(key, value, true);
                    storeCallbacks.newKeyInsertedToCache(key, value.timestamp()); // i.e., move latest value into segment
                } else if (value.timestamp() < oldValue.timestamp()) {
                    // insert into underlying store, update cache with latest (clean)
                    putToCacheNonContext(key, oldValue, false);
                    storeCallbacks.bypassCache(key, value, oldValue.timestamp()); // i.e., put to store bypassing latest value (known to be safe here)
                } else {
                    // insert into underlying store, update cache with latest (clean)
                    putToCache(key, value, false);
                    wrapped().put(key, value);
                }
            } else {
                // null could have earlier or later timestamp. insert into underlying store and do not try to cache
                wrapped().put(key, value);
            }
        }

        StoreQueryUtils.updatePosition(position, context); // TODO: what's this?
    }

    private void putToCache(final Bytes key, final ValueAndTimestamp<byte[]> value, final boolean isDirty) {
        context.cache().put(
            cacheName,
            key,
            new LRUCacheEntry(
                value.value(),
                context.headers(),
                isDirty,
                context.offset(),
                value.timestamp(),
                context.partition(),
                context.topic()));
    }

    private void putToCacheNonContext(final Bytes key, final ValueAndTimestamp<byte[]> value, final boolean isDirty) {
        context.cache().put(cacheName, key, new LRUCacheEntry(value.value(), new RecordHeaders(), isDirty, -1, value.timestamp(), -1, ""));
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
            final boolean vIsNull = v == null || v.value() == null; // TODO: extract into helper method on ValueAndTimestamp
            if (vIsNull) {
                putInternal(key, value);
            }
            return vIsNull ? null : v;
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
                putInternal(entry.key, entry.value); // TODO(note): this used to just be put() but looks like it should be putInternal() instead?
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
        putInternal(key, ValueAndTimestamp.makeAllowNullable(null, context.timestamp()));
        return v == null || v.value() == null ? null : v;
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
            final ValueAndTimestamp<byte[]> value = getInternal(key);
            if (value == null || value.value() == null) {
                return null;
            }
            return value;
        } finally {
            theLock.unlock();
        }
    }

    // TODO: de-dup from the other get(), and similarly for the others
    @Override
    public ValueAndTimestamp<byte[]> get(final Bytes key, final long timestampTo) {
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
            final ValueAndTimestamp<byte[]> value = getInternal(key, timestampTo);
            if (value == null || value.value() == null) {
                return null;
            }
            return value;
        } finally {
            theLock.unlock();
        }
    }

    // this may return a null wrapped in a timestamp. it is caller's responsibility to filter for this if applicable.
    private ValueAndTimestamp<byte[]> getInternal(final Bytes key) {
        LRUCacheEntry entry = null;
        if (context.cache() != null) {
            entry = context.cache().get(cacheName, key);
        }
        if (entry == null) {
            final ValueAndTimestamp<byte[]> rawValue = wrapped().get(key); // TODO: would be better to distinguish between null and null wrapped with timestamp here, since one can be cached
            if (rawValue == null) {
                return null;
            }
            // only update the cache if this call is on the streamThread
            // as we don't want other threads to trigger an eviction/flush
            if (Thread.currentThread().equals(streamThread)) {
                putToCacheNonContext(key, rawValue, false);
            }
            return rawValue;
        } else {
            return ValueAndTimestamp.makeAllowNullable(entry.value(), entry.context().timestamp()); // TODO(note): cache may contain null entries (with timestamps) from putInternal()
        }
    }

    // this may return a null wrapped in a timestamp. it is caller's responsibility to filter for this if applicable.
    private ValueAndTimestamp<byte[]> getInternal(final Bytes key, final long timestampTo) {
        LRUCacheEntry entry = null;
        if (context.cache() != null) {
            entry = context.cache().get(cacheName, key);
        }
        if (entry != null && entry.context().timestamp() <= timestampTo) {
            return ValueAndTimestamp.makeAllowNullable(entry.value(), entry.context().timestamp());
        } else {
            final ValueAndTimestamp<byte[]> rawValue = wrapped().get(key, timestampTo);
            if (rawValue == null) {
                return null;
            }
            // only update the cache if this call is on the streamThread
            // as we don't want other threads to trigger an eviction/flush
            if (Thread.currentThread().equals(streamThread)) {
                // check if this entry is the latest, and insert into cache if so
                // TODO: optimize this into a single call with the above?
                final ValueAndTimestamp<byte[]> latestValue = wrapped().get(key);
                if (rawValue.equals(latestValue)) {
                    putToCacheNonContext(key, rawValue, false);
                }
            }
            return rawValue;
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
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(final Bytes from,
                                                                    final Bytes to,
                                                                    final long timestampTo) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator = wrapped().range(from, to, timestampTo);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().range(cacheName, from, to);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(
            filterByTimestamp(cacheIterator, timestampTo), // TODO: update interface rather than applying this after-the-fact hack
            storeIterator,
            true
        );
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
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(final Bytes from,
                                                                           final Bytes to,
                                                                           final long timestampTo) {
        if (Objects.nonNull(from) && Objects.nonNull(to) && from.compareTo(to) > 0) {
            LOG.warn("Returning empty iterator for fetch with invalid key range: from > to. " +
                "This may be due to range arguments set in the wrong order, " +
                "or serdes that don't preserve ordering when lexicographically comparing the serialized bytes. " +
                "Note that the built-in numerical serdes do not follow this for negative numbers");
            return KeyValueIterators.emptyIterator();
        }

        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator = wrapped().reverseRange(from, to, timestampTo);
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().reverseRange(cacheName, from, to);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(
            filterByTimestamp(cacheIterator, timestampTo), // TODO: same as above
            storeIterator,
            false
        );
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
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all(final long timestampTo) {
        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator =
            new DelegatingPeekingKeyValueIterator<>(this.name(), wrapped().all(timestampTo));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().all(cacheName);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(
            filterByTimestamp(cacheIterator, timestampTo), // TODO: same as above
            storeIterator,
            true
        );
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
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll(final long timestampTo) {
        validateStoreOpen();
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator =
            new DelegatingPeekingKeyValueIterator<>(this.name(), wrapped().reverseAll(timestampTo));
        final ThreadCache.MemoryLRUCacheBytesIterator cacheIterator = context.cache().reverseAll(cacheName);
        return new MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(
            filterByTimestamp(cacheIterator, timestampTo), // TODO: same as above
            storeIterator,
            false
        );
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

    @Override
    public void deleteHistory(final long timestampTo) {
        wrapped().deleteHistory(timestampTo);
    }

    private static PeekingKeyValueIterator<Bytes, LRUCacheEntry> filterByTimestamp(
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
        final long timestampTo
    ) {
        return new PeekingKeyValueIterator<Bytes, LRUCacheEntry>() {
            @Override
            public boolean hasNext() {
                while (cacheIterator.hasNext() && !satisfiesTimeFilter(cacheIterator.peekNext().value)) {
                    cacheIterator.next();
                }
                return cacheIterator.hasNext();
            }

            @Override
            public KeyValue<Bytes, LRUCacheEntry> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return cacheIterator.next();
            }

            @Override
            public void close() {
                cacheIterator.close();
            }

            @Override
            public Bytes peekNextKey() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return cacheIterator.peekNextKey();
            }

            @Override
            public KeyValue<Bytes, LRUCacheEntry> peekNext() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return cacheIterator.peekNext();
            }

            private boolean satisfiesTimeFilter(final LRUCacheEntry entry) {
                return entry.context().timestamp() <= timestampTo;
            }
        };
    }
}

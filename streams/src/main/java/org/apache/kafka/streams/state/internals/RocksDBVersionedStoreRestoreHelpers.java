package org.apache.kafka.streams.state.internals;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStore.LatestValueSchema;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStore.SegmentValueSchema;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStore.VersionedStoreClient;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreRestoreHelpers.MemoryLRUCache.EldestEntryRemovalListener;

public class RocksDBVersionedStoreRestoreHelpers {

    private static final int MAX_CACHE_SIZE = 200; // TODO: fix

    private final LatestValueSchema latestValueSchema;
    private final SegmentValueSchema segmentValueSchema;
    private final Function<Long, Long> segmentIdGetter;

    private final MemoryLRUCache cache;

    RocksDBVersionedStoreRestoreHelpers(
        final LatestValueSchema latestValueSchema,
        final SegmentValueSchema segmentValueSchema,
        final Function<Long, Long> segmentIdGetter,
        final EldestEntryRemovalListener cacheRemovalListener
    ) {
        this.latestValueSchema = latestValueSchema;
        this.segmentValueSchema = segmentValueSchema;
        this.segmentIdGetter = segmentIdGetter;
        this.cache = new MemoryLRUCache(cacheRemovalListener, MAX_CACHE_SIZE);
    }

    // TODO(here): add methods for actually using the cache?

    private class RocksDBVersionedStoreRestoreClient<T> implements VersionedStoreClient<Long> {

        private final VersionedStoreClient<T> readOnlyDelegate;

        RocksDBVersionedStoreRestoreClient(final VersionedStoreClient<T> readOnlyDelegate) {
            this.readOnlyDelegate = readOnlyDelegate;
        }

        @Override
        public byte[] getLatestValue(Bytes key) { // TODO: should this be typed as value and timestamp in order to avoid serializing/deserializing timestamp over and over?
            final CacheEntry entry = cache.get(key);
            final byte[] value;
            if (entry == null) {
                // read from store
                value = readOnlyDelegate.getLatestValue(key);

                // add to cache
                cache.put(key, new CacheEntry(new MaybeDirty<>(new CacheLatestValue(value), false)));
            } else {
                if (entry.getLatest() == null) { // TODO: remove
                    throw new IllegalStateException("latest value should always be read first");
                }

                // get from cache
                value = entry.getLatest().value().get();
            }
            return value;
        }

        @Override
        public void putLatestValue(Bytes key, byte[] value) {
            final CacheEntry entry = cache.get(key);
            if (entry == null) {
                cache.put(key, new CacheEntry(new MaybeDirty<>(new CacheLatestValue(value), true)));
            } else {
                entry.updateLatest(new MaybeDirty<>(new CacheLatestValue(value), true));
            }
        }

        @Override
        public void deleteLatestValue(Bytes key) {
            putLatestValue(key, null);
        }

        @Override
        public Long getOrCreateSegmentIfLive(long segmentId, ProcessorContext unusedContext, long unusedTs) {
            // TODO: should we incorporate stream time here? the infra is set up to allow it so I guess we might as well
            return segmentId;
        }

        @Override
        public List<Long> getReverseSegments(long timestampFrom, Bytes key) {
            final List<Long> dbSegments = readOnlyDelegate.getReverseSegments(timestampFrom, key).stream()
                .map(readOnlyDelegate::getIdForSegment)
                .collect(Collectors.toList());
            if (cache.get(key) == null) {
                return dbSegments;
            }

            final List<Long> cacheSegments = cache.get(key).getReverseSegments(segmentIdGetter.apply(timestampFrom));

            // merge segments from db with segments from cache
            final List<Long> allSegments = new ArrayList<>();
            int dbInd = 0;
            int cacheInd = 0;
            while (dbInd < dbSegments.size() && cacheInd < cacheSegments.size()) {
                final Long dbVal = dbSegments.get(dbInd);
                final Long cacheVal = cacheSegments.get(cacheInd);
                if (dbVal > cacheVal) {
                    allSegments.add(dbVal);
                    dbInd++;
                } else if (dbVal < cacheVal) {
                    allSegments.add(cacheVal);
                    cacheInd++;
                } else {
                    allSegments.add(dbVal);
                    dbInd++;
                    cacheInd++;
                }
            }
            while (dbInd < dbSegments.size()) {
                allSegments.add(dbSegments.get(dbInd));
                dbInd++;
            }
            while (cacheInd < cacheSegments.size()) {
                allSegments.add(cacheSegments.get(cacheInd));
                cacheInd++;
            }
            return allSegments;
        }

        @Override
        public byte[] getFromSegment(Long segment, Bytes key) {
            // first check cache
            final CacheEntry entry = cache.get(key);
            if (entry == null) { // TODO: move this up / handle differently
                throw new IllegalStateException("should not get from segment before getting latest value");
            }

            MaybeDirty<CacheSegmentValue> cacheValue = entry.getSegment(segment);
            if (cacheValue != null) {
                return cacheValue.value().get();
            }

            // not found in cache, check db instead
            final byte[] dbValue;
            final T delegateSegment = readOnlyDelegate.getSegmentIfPresent(segment);
            if (delegateSegment == null) {
                dbValue = null;
            } else {
                dbValue = readOnlyDelegate.getFromSegment(delegateSegment, key);
            }
            entry.putSegment(segment, new MaybeDirty<>(new CacheSegmentValue(dbValue), false));
            return dbValue;
        }

        @Override
        public void putToSegment(Long segment, Bytes key, byte[] value) {
            final CacheEntry entry = cache.get(key);
            if (entry == null) { // TODO: move this up / handle differently
                throw new IllegalStateException("should not put to segment before getting latest value");
            }

            entry.putSegment(segment, new MaybeDirty<>(new CacheSegmentValue(value), true));
        }

        @Override
        public long getIdForSegment(Long segment) {
            if (segment == null) { // TODO: remove
                throw new IllegalStateException("segment cannot be null");
            }
            return segment;
        }

        @Override
        public Long getSegmentIfPresent(long segmentId) {
            return segmentId;
        }
    }

    private static class CacheEntry {
        private MaybeDirty<CacheLatestValue> latestValue;
        private final TreeMap<Long, MaybeDirty<CacheSegmentValue>> segmentValues;

        CacheEntry(final MaybeDirty<CacheLatestValue> latestValue) {
            this.latestValue = latestValue;
            // store in reverse-sorted order, to make getReverseSegments() more efficient. TODO: check
            this.segmentValues = new TreeMap<>((x, y) -> -Long.compare(x, y));
        }

        MaybeDirty<CacheLatestValue> getLatest() {
            return latestValue;
        }

        // returns null if not present
        MaybeDirty<CacheSegmentValue> getSegment(final long segmentId) {
            return segmentValues.get(segmentId);
        }

        // returns in reverse-sorted order; subject to change in the future.
        Map<Long, MaybeDirty<CacheSegmentValue>> getAllSegments() {
            return segmentValues; // TODO: return copy? expose iterator instead?
        }

        List<Long> getReverseSegments(final long segmentId) {
            return new ArrayList<>(segmentValues.tailMap(segmentId).keySet());
        }

        void updateLatest(final MaybeDirty<CacheLatestValue> latestValue) {
            this.latestValue = latestValue;
        }

        void putSegment(final long segmentId, final MaybeDirty<CacheSegmentValue> segmentValue) {
            this.segmentValues.put(segmentId, segmentValue);
        }
    }

    private static class CacheLatestValue {
        private final byte[] timestampAndValue;

        CacheLatestValue(final byte[] timestampAndValue) {
            this.timestampAndValue = timestampAndValue;
        }

        // could be null
        public byte[] get() {
            return timestampAndValue;
        }
    }

    private static class CacheSegmentValue {
        private final byte[] value;

        CacheSegmentValue(final byte[] value) {
            this.value = value;
        }

        // could be null
        public byte[] get() {
            return value;
        }
    }

    private static class MaybeDirty<T> {
        private final T value;
        private final boolean isDirty;

        MaybeDirty(final T value, final boolean isDirty) {
            this.value = value;
            this.isDirty = isDirty;
        }

        // could be null
        public T value() {
            return value;
        }

        public boolean isDirty() {
            return isDirty;
        }
    }

    // cribbed from streams implementation of MemoryLRUCache
    static class MemoryLRUCache {

        interface EldestEntryRemovalListener {
            void apply(Bytes key, CacheEntry value);
        }

        private final Map<Bytes, CacheEntry> map;
        private final EldestEntryRemovalListener listener;

        MemoryLRUCache(final EldestEntryRemovalListener listener, final int maxCacheSize) {

            // leave room for one extra entry to handle adding an entry before the oldest can be removed
            this.map = new LinkedHashMap<Bytes, CacheEntry>(maxCacheSize + 1, 1.01f, true) {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(final Map.Entry<Bytes, CacheEntry> eldest) {
                    final boolean evict = super.size() > maxCacheSize;
                    if (evict && listener != null) {
                        listener.apply(eldest.getKey(), eldest.getValue());
                    }
                    return evict;
                }
            };

            this.listener = listener;
        }

        public synchronized CacheEntry get(final Bytes key) { // TODO: restore should be single-threaded so this synchronization shouldn't be needed?
            Objects.requireNonNull(key);

            return this.map.get(key);
        }

        public synchronized void put(final Bytes key, final CacheEntry value) {
            Objects.requireNonNull(key);
            if (value == null) {
                delete(key);
            } else {
                this.map.put(key, value);
            }
        }

        public synchronized CacheEntry delete(final Bytes key) {
            Objects.requireNonNull(key);
            return this.map.remove(key);
        }

        public KeyValueIterator<Bytes, CacheEntry> all() {
            // TODO: implement
        }

    }
}

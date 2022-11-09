package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class MergedSortedTimeAwareCacheKeyValueBytesStoreIterator
    extends AbstractMergedSortedCacheStoreIterator<Bytes, Bytes, ValueAndTimestamp<byte[]>, ValueAndTimestamp<byte[]>>{

    MergedSortedTimeAwareCacheKeyValueBytesStoreIterator(
        final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
        final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> storeIterator,
        final boolean forward
    ) {
        super(cacheIterator, storeIterator, forward);
    }

    @Override
    public KeyValue<Bytes, ValueAndTimestamp<byte[]>> deserializeStorePair(final KeyValue<Bytes, ValueAndTimestamp<byte[]>> pair) {
        return pair;
    }

    @Override
    Bytes deserializeCacheKey(final Bytes cacheKey) {
        return cacheKey;
    }

    @Override
    ValueAndTimestamp<byte[]> deserializeCacheValue(final LRUCacheEntry cacheEntry) {
        return ValueAndTimestamp.make(cacheEntry.value(), cacheEntry.context().timestamp());
    }

    @Override
    public Bytes deserializeStoreKey(final Bytes key) {
        return key;
    }

    @Override
    public int compare(final Bytes cacheKey, final Bytes storeKey) {
        return cacheKey.compareTo(storeKey);
    }
}

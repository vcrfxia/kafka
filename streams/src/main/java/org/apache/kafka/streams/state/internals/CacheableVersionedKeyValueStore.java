package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;

// TODO: rethink how this wiring between cache and underlying store works
public interface CacheableVersionedKeyValueStore<K, V> extends VersionedKeyValueStoreInternal<K, V> {

    // represents new value replacing existing value from cache, so previous value now
    // needs to be written to underlying store.
    // nextTimestamp represents timestamp of cache entry (i.e., latest store entry)
    void replaceFromCache(K key, ValueAndTimestamp<V> value, long nextTimestamp);

    // represents existing value in cache (could be either clean or dirty), and this is
    // an older record which should be put straight to the underlying store.
    // nextTimestamp represents timestamp of cache entry (i.e., latest store entry),
    // and does not change as a result of this call but is provided for convenience
    void bypassCache(K key, ValueAndTimestamp<V> value, long nextTimestamp);

    // a new record having been inserted to the cache. underlying store does not have the
    // record yet but is still notified in order to bookmark that a new record exists.
    // nextTimestamp represents timestamp of cache entry (i.e., latest store entry)
    void newKeyInsertedToCache(K key, long nextTimestamp);

}

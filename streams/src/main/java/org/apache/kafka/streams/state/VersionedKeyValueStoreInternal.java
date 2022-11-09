package org.apache.kafka.streams.state;

public interface VersionedKeyValueStoreInternal<K, V> extends TimestampedKeyValueStore<K, V> {
    ValueAndTimestamp<V> get(K key, long timestampTo);

    // additional methods we could choose to support if we wanted, but aren't strictly necessary
    KeyValueIterator<K, ValueAndTimestamp<V>> range(K from, K to, long timestampTo);
    KeyValueIterator<K, ValueAndTimestamp<V>> reverseRange(K from, K to, long timestampTo);
    KeyValueIterator<K, ValueAndTimestamp<V>> all(long timestampTo);
    KeyValueIterator<K, ValueAndTimestamp<V>> reverseAll(long timestampTo);
}

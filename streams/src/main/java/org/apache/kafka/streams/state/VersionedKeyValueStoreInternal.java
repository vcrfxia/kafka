package org.apache.kafka.streams.state;

public interface VersionedKeyValueStoreInternal<K, V> extends TimestampedKeyValueStore<K, V> {
    ValueAndTimestamp<V> get(K key, long timestampTo);
}

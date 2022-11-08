package org.apache.kafka.streams.state;

import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.processor.StateStore;

public interface VersionedKeyValueStore<K, V> extends StateStore {
    void put(K key, V value, long timestamp);
    ValueAndTimestamp<V> get(K key);
    ValueAndTimestamp<V> get(K key, long timestampTo);

    // additional methods we could choose to support if we wanted, but aren't strictly necessary
    ValueAndTimestamp<V> putIfAbsent(K key, V value, long timestamp);
    void putAll(List<KeyValueTimestamp<K, V>> entries);
    V delete(K key);
    KeyValueTimestampIterator<K, V> range(K from, K to);
    KeyValueTimestampIterator<K, V> range(K from, K to, long timestampTo);
    KeyValueTimestampIterator<K, V> reverseRange(K from, K to);
    KeyValueTimestampIterator<K, V> reverseRange(K from, K to, long timestampTo);
    KeyValueTimestampIterator<K, V> all();
    KeyValueTimestampIterator<K, V> all(long timestampTo);
    KeyValueTimestampIterator<K, V> reverseAll();
    KeyValueTimestampIterator<K, V> reverseAll(long timestampTo);
    <PS extends Serializer<P>, P> KeyValueTimestampIterator<K, byte[]> prefixScan(P prefix, PS prefixKeySerializer);
    <PS extends Serializer<P>, P> KeyValueTimestampIterator<K, byte[]> prefixScan(P prefix, PS prefixKeySerializer, long timestampTo);
    long approximateNumEntries();
}

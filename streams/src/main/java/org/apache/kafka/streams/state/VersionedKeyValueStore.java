package org.apache.kafka.streams.state;

import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStore;

/**
 * A key-value store that stores multiple record versions per key, and supports timestamp-based
 * retrieval operations to return the latest record (per key) as of a specified timestamp.
 * Only one record is stored per key and timestamp, i.e., a second call to
 * {@link #put(Object, Object, long)} with the same key and timestamp will replace the first.
 * <p>
 * Each store instance has an associated, fixed-duration "history retention" which specifies
 * how long old record versions should be kept for. In particular, a versioned store guarantees
 * to return accurate results for calls to {@link #get(Object, long)} where the provided timestamp
 * bound is within history retention of the current observed stream time. (Queries with timestamp
 * bound older than the specified history retention are considered invalid.)
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface VersionedKeyValueStore<K, V> extends StateStore {

    /**
     * Add a new record version associated with this key.
     *
     * @param key       The key
     * @param value     The value, it can be {@code null};
     *                  if the serialized bytes are also {@code null} it is interpreted as a delete
     * @param timestamp The timestamp for this record version
     * @throws NullPointerException If {@code null} is used for key.
     */
    void put(K key, V value, long timestamp);

    /**
     * Delete the value associated with this key from the store, at the specified timestamp
     * (if there is such a value), and return the deleted value.
     * <p>
     * This operation is semantically equivalent to {@link #get(Object, long)} #get(key, timestamp))}
     * followed by {@link #put(Object, Object, long) #put(key, null, timestamp)}.
     *
     * @param key       The key
     * @param timestamp The timestamp for this delete
     * @return The value and timestamp of the latest record associated with this key
     *         as of the deletion timestamp (inclusive), or {@code null} if any of
     *         (1) the store contains no records for this key, (2) the latest record
     *         for this key as of the deletion timestamp is a tombstone, or
     *         (3) the deletion timestamp is older than this store's history retention
     *         (i.e., this store no longer contains data for the provided timestamp).
     * @throws NullPointerException If {@code null} is used for key.
     */
    VersionedRecord<V> delete(K key, long timestamp);

    /**
     * Get the latest (by timestamp) record associated with this key.
     *
     * @param key The key to fetch
     * @return The value and timestamp of the latest record associated with this key, or
     *         {@code null} if either (1) the store contains no records for this key or (2) the
     *         latest record for this key is a tombstone.
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    VersionedRecord<V> get(K key);

    /**
     * Get the latest record associated with this key with timestamp not exceeding the specified
     * timestamp bound.
     *
     * @param key           The key to fetch
     * @param asOfTimestamp The timestamp bound. This bound is inclusive; if a record
     *                      (for the specified key) exists with this timestamp, then
     *                      this is the record that will be returned.
     * @return The value and timestamp of the latest record associated with this key
     *         satisfying the provided timestamp bound, or {@code null} if any of
     *         (1) the store contains no records for this key, (2) the latest record
     *         for this key satisfying the provided timestamp bound is a tombstone, or
     *         (3) the provided timestamp bound is older than this store's history retention
     *         (i.e., this store no longer contains data for the provided timestamp bound).
     * @throws NullPointerException       If null is used for key.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    VersionedRecord<V> get(K key, long asOfTimestamp);

    // additional methods we could choose to support if we wanted, but aren't strictly necessary
    ValueAndTimestamp<V> putIfAbsent(K key, V value, long timestamp);
    void putAll(List<KeyValueTimestamp<K, V>> entries);

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

package org.apache.kafka.streams.state;

import org.apache.kafka.streams.errors.InvalidStateStoreException;

// TODO(note): ideally extend the interfaces KeyValueStore extends instead,
// and update put() signatures to include timestamp. until then, extend
// TimestampedKeyValueStore instead so timestamp is accessible from
// existing interfaces
public interface VersionedKeyValueStoreInternal<K, V> extends TimestampedKeyValueStore<K, V> {

  /**
   * Get the value corresponding to this key, as of the provided timestamp.
   *
   * @param key         The key to fetch
   * @param timestampTo Timestamp for this query
   * @return The value or null if no value is found.
   * @throws NullPointerException       If null is used for key.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  ValueAndTimestamp<V> get(K key, long timestampTo);

  /**
   * Get an iterator over a given range of keys, as of the provided timestamp.
   * This iterator must be closed after use.
   * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
   * and must not return null values.
   * Order is not guaranteed as bytes lexicographical ordering might not represent key order.
   *
   * @param from        The first key that could be in the range, where iteration starts from.
   *                    A null value indicates that the range starts with the first element
   *                    in the store.
   * @param to          The last key that could be in the range, where iteration ends.
   *                    A null value indicates that the range ends with the last element
   *                    in the store.
   * @param timestampTo Timestamp for this query
   * @return The iterator for this range, from smallest to largest bytes.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  KeyValueIterator<K, ValueAndTimestamp<V>> range(K from, K to, long timestampTo);

  /**
   * Get a reverse iterator over a given range of keys, as of the provided timestamp.
   * This iterator must be closed after use.
   * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
   * and must not return null values.
   * Order is not guaranteed as bytes lexicographical ordering might not represent key order.
   *
   * @param from The first key that could be in the range, where iteration ends.
   *             A null value indicates that the range starts with the first element in the store.
   * @param to   The last key that could be in the range, where iteration starts from.
   *             A null value indicates that the range ends with the last element in the store.
   * @param timestampTo Timestamp for this query
   * @return The reverse iterator for this range, from largest to smallest key bytes.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  default KeyValueIterator<K, ValueAndTimestamp<V>> reverseRange(K from, K to, long timestampTo) {
    throw new UnsupportedOperationException();
  }

  /**
   * Return an iterator over all keys in this store, as of the provided timestamp.
   * This iterator must be closed after use.
   * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
   * and must not return null values.
   * Order is not guaranteed as bytes lexicographical ordering might not represent key order.
   *
   * @param timestampTo Timestamp for this query
   * @return An iterator of all key/value pairs in the store, from smallest to largest bytes.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  KeyValueIterator<K, ValueAndTimestamp<V>> all(long timestampTo);

  /**
   * Return a reverse iterator over all keys in this store, as of the provided timestamp.
   * This iterator must be closed after use.
   * The returned iterator must be safe from {@link java.util.ConcurrentModificationException}s
   * and must not return null values.
   * Order is not guaranteed as bytes lexicographical ordering might not represent key order.
   *
   * @param timestampTo Timestamp for this query
   * @return An reverse iterator of all key/value pairs in the store, from largest to smallest key bytes.
   * @throws InvalidStateStoreException if the store is not initialized
   */
  default KeyValueIterator<K, ValueAndTimestamp<V>> reverseAll(long timestampTo) {
    throw new UnsupportedOperationException();
  }
}

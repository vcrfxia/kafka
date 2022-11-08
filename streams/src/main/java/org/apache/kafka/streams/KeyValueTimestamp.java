package org.apache.kafka.streams;

import java.util.Objects;

public class KeyValueTimestamp<K, V> {

    public final K key;
    public final V value;
    public final long timestamp;

    public KeyValueTimestamp(final K key, final V value, final long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public static <K, V> KeyValueTimestamp<K, V> of(final K key, final V value, final long timestamp) {
        return new KeyValueTimestamp<>(key, value, timestamp);
    }

    @Override
    public String toString() {
        return "KeyValueTimestamp(" + key + ", " + value + ", " + timestamp + ")";
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof KeyValueTimestamp)) {
            return false;
        }

        final KeyValueTimestamp other = (KeyValueTimestamp) obj;
        return Objects.equals(key, other.key) && Objects.equals(value, other.value) && timestamp == other.timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}

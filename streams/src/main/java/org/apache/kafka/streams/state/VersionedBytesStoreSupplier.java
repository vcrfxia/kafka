package org.apache.kafka.streams.state;

/**
 * A store supplier that can be used to create one or more versioned key-value stores,
 * specifically, {@link VersionedBytesStore} instances.
 * <p>
 * Rather than representing the returned store as a {@link VersionedKeyValueStore} of
 * type &lt;Bytes, byte[]&gt;, this supplier interface represents the returned store as a
 * {@link KeyValueStore} of type &lt;Bytes, byte[]&gt; (via {@link VersionedBytesStore}) in order to be compatible with
 * existing DSL methods for passing key-value stores such as {@link StreamsBuilder#table(String, Materialized)}
 * and {@link KTable#filter(Predicate, Materialized)}. A {@code VersionedKeyValueStore<Bytes, byte[]>}
 * is represented as a {@code KeyValueStore KeyValueStore<Bytes, byte[]>} by interpreting the
 * value bytes as containing record timestamp information in addition to raw record values.
 */
public interface VersionedBytesStoreSupplier extends KeyValueBytesStoreSupplier {

    /**
     * Returns the history retention (in milliseconds) that stores created from this supplier will have.
     * This value is used to set compaction configs on store changelog topics (if relevant).
     *
     * @return history retention, i.e., length of time that old record versions are available for
     *         query from a versioned store
     */
    long historyRetentionMs();
}
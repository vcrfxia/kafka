package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.Bytes;

/**
 * A representation of a versioned key-value store as a {@link KeyValueStore} of type &lt;Bytes, byte[]&gt;.
 * See {@link VersionedBytesStoreSupplier} for more.
 */
public interface VersionedBytesStore extends KeyValueStore<Bytes, byte[]>, TimestampedBytesStore {

    /**
     * The analog of {@link VersionedKeyValueStore#get(Object, long)}.
     */
    byte[] get(Bytes key, long timestampTo);

    // add other methods specific to versioned stores

}

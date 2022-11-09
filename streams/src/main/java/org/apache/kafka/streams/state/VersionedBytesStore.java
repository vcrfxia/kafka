package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.Bytes;

public interface VersionedBytesStore extends KeyValueStore<Bytes, byte[]>, TimestampedBytesStore {

    byte[] get(Bytes key, long timestampTo);

    // add other methods specific to versioned stores

}

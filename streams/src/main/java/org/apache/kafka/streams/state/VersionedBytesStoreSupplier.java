package org.apache.kafka.streams.state;

import org.apache.kafka.common.utils.Bytes;

public interface VersionedBytesStoreSupplier extends StoreSupplier<VersionedKeyValueStore<Bytes, byte[]>> {
    long historyRetentionMs();
}

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

public class RocksDBVersionedStore implements VersionedKeyValueStore<Bytes, byte[]> {

    // implementation abstracted out for brevity, but this is where all the actual magic happens
    // using the clean interfaces from VersionedKeyValueStore

}

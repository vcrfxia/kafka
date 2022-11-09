package org.apache.kafka.streams;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Test;

public class VersionedStoreInterfacesTest {

    @Test
    public void baseline() {
        Materialized<String, String, KeyValueStore<Bytes, byte[]>> m = Materialized.as("name");
        new StreamsBuilder().table("t", m);
    }

    @Test
    public void inheritance() {
        Materialized<String, String, OtherKeyValueStore<Bytes, byte[]>> m = Materialized.as("name");
        new StreamsBuilder().table("t", m);
    }

    public interface OtherKeyValueStore<K, V> extends KeyValueStore<K, V> {
    }
}
package org.apache.kafka.streams.state;

import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

public class VersionedInterfacesTest {
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private final Properties config = Utils.mkProperties(Utils.mkMap(
        Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getSimpleName().toLowerCase(Locale.getDefault())),
        Utils.mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
        Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bogus")
    ));

    @Test
    public void testWithKVStore() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> m =
            Materialized.as(Stores.persistentKeyValueStore("kvstore"));
        builder
            .table(
                "input",
                Consumed.with(Serdes.String(), Serdes.String()),
                m.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .filter(
                (k, v) -> true
            )
            .toStream()
            .foreach((k, v) -> System.out.println("filtered K: " + k + ", V: " + v));
        final Topology topology = builder.build();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v2", 1L);
            inputTopic.pipeInput("k2", "v1", 2L);
        }
    }

    @Test
    public void testWithVersionedStore() {
        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> m =
            Materialized.as(Stores.persistentVersionedKeyValueStore("kvstore", Duration.ofMillis(1000), Duration.ofMillis(100)));
        builder
            .table(
                "input",
                Consumed.with(Serdes.String(), Serdes.String()),
                m.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            )
            .filter(
                (k, v) -> true
            )
            .toStream()
            .foreach((k, v) -> System.out.println("filtered K: " + k + ", V: " + v));
        final Topology topology = builder.build();

        try (final TopologyTestDriver driver = new TopologyTestDriver(topology, config)) {
            final TestInputTopic<String, String> inputTopic = driver.createInputTopic("input", STRING_SERIALIZER, STRING_SERIALIZER);
            inputTopic.pipeInput("k1", "v1", 0L);
            inputTopic.pipeInput("k1", "v2", 1L);
            inputTopic.pipeInput("k2", "v1", 2L);
        }
    }
}
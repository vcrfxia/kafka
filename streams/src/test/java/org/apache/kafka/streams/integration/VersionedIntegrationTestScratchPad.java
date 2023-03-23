package org.apache.kafka.streams.integration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.integration.VersionedKeyValueStoreIntegrationTest.VersionedStoreContentCheckerProcessor;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedRecord;
import org.junit.Before;
import org.junit.Test;

public class VersionedIntegrationTestScratchPad {

    private static final String STORE_NAME = "versioned-store";
    private static final long HISTORY_RETENTION = 3600_000L;

    private String inputStream;
    private String globalTableTopic;
    private String outputStream;
    private long baseTimestamp;

    @Before
    public void beforeTest() {
        inputStream = "input-stream";
        globalTableTopic = "global-table";
        outputStream = "output-stream";

        baseTimestamp = System.currentTimeMillis();
    }

    @Test
    public void topologyTestDriver() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .addStateStore(
                Stores.versionedKeyValueStoreBuilder(
                    Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION)),
                    Serdes.Integer(),
                    Serdes.String()
                )
            )
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(true), STORE_NAME)
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        try (final TopologyTestDriver versionedTopology = new TopologyTestDriver(streamsBuilder.build())) {
            final VersionedKeyValueStore<Integer, String> store = versionedTopology.getVersionedKeyValueStore(STORE_NAME);

            final TestInputTopic<Integer, String> inputTopic = versionedTopology.createInputTopic(inputStream, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(1, "a0", baseTimestamp);

            assertThat(store.get(1), equalTo(new VersionedRecord<>("a0", baseTimestamp)));
        }
    }

    @Test
    public void topologyTestDriverGlobal() {
        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        streamsBuilder
            .globalTable(
                globalTableTopic,
                Consumed.with(Serdes.Integer(), Serdes.String()),
                Materialized
                    .<Integer, String>as(Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION)))
                    .withKeySerde(Serdes.Integer())
                    .withValueSerde(Serdes.String())
            );
        streamsBuilder
            .stream(inputStream, Consumed.with(Serdes.Integer(), Serdes.String()))
            .process(() -> new VersionedStoreContentCheckerProcessor(false)) // did not pass expected data into processor, so verification will fail but that's fine since the isn't checking that
            .to(outputStream, Produced.with(Serdes.Integer(), Serdes.Integer()));

        try (final TopologyTestDriver versionedTopology = new TopologyTestDriver(streamsBuilder.build())) {
            final VersionedKeyValueStore<Integer, String> store = versionedTopology.getVersionedKeyValueStore(STORE_NAME);

            final TestInputTopic<Integer, String> inputTopic = versionedTopology.createInputTopic(globalTableTopic, new IntegerSerializer(), new StringSerializer());
            inputTopic.pipeInput(1, "a0", baseTimestamp);

            assertThat(store.get(1), equalTo(new VersionedRecord<>("a0", baseTimestamp)));
        }
    }
}

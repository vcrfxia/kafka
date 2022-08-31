package org.apache.kafka.streams.state.internals;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.mock;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.File;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RocksDBVersionedStoreTest {

    private static final String STORE_NAME = "myversionedrocks";
    private static final String METRICS_SCOPE = "versionedrocksdb";
    private static final long HISTORY_RETENTION = 300_000L;
    private static final long SEGMENT_INTERVAL = HISTORY_RETENTION / 2;

    private static final long BASE_TIMESTAMP = 10L;

    // TODO: de-dup from RocksDBStoreTest
    private File dir;
    private final Time time = new MockTime();
    private final Serializer<String> stringSerializer = new StringSerializer();
    private final Deserializer<String> stringDeserializer = new StringDeserializer();
    private final ValueAndTimestampSerializer<String> stringAndTsSerializer = new ValueAndTimestampSerializer<>(stringSerializer);
    private final ValueAndTimestampDeserializer<String> stringAndTsDeserializer = new ValueAndTimestampDeserializer<>(stringDeserializer);

    private final RocksDBMetricsRecorder metricsRecorder = mock(RocksDBMetricsRecorder.class);

    protected InternalMockProcessorContext context;
    protected RocksDBVersionedStore store;

    @Before
    public void before() {
        dir = TestUtils.tempDirectory();
        context = new InternalMockProcessorContext<>(
            dir,
            Serdes.String(),
            Serdes.String(),
            new StreamsConfig(StreamsTestUtils.getStreamsConfig())
        );
        context.setTime(BASE_TIMESTAMP); // TODO: ?

        store = new RocksDBVersionedStore(STORE_NAME, METRICS_SCOPE, HISTORY_RETENTION, SEGMENT_INTERVAL);
        store.init((StateStoreContext) context, store);
    }

    @After
    public void after() {
        store.close();
    }

    private void putStore(final String key, final String value, final long timestamp) {
        store.put(
            new Bytes(key.getBytes(UTF_8)),
            ValueAndTimestamp.makeAllowNullable(value == null ? null : value.getBytes(UTF_8), timestamp)
        );
    }

    private String getValueFromStore(final String key) {
        final ValueAndTimestamp<String> valueAndTimestamp = getFromStore(key);
        return valueAndTimestamp == null ? null : valueAndTimestamp.value();
    }

    private String getValueFromStore(final String key, final long timestampTo) {
        final ValueAndTimestamp<String> valueAndTimestamp = getFromStore(key, timestampTo);
        return valueAndTimestamp == null ? null : valueAndTimestamp.value();
    }

    // TODO: de-dup from below
    private ValueAndTimestamp<String> getFromStore(final String key) {
        final ValueAndTimestamp<byte[]> valueAndTimestamp
            = store.get(new Bytes(stringSerializer.serialize(null, key)));
            //= store.get(new Bytes(key.getBytes(UTF_8)));
        return valueAndTimestamp == null
            ? null
            : ValueAndTimestamp.make(
            stringDeserializer.deserialize(null, valueAndTimestamp.value()),
            valueAndTimestamp.timestamp());
    }

    private ValueAndTimestamp<String> getFromStore(final String key, final long timestampTo) {
        final ValueAndTimestamp<byte[]> valueAndTimestamp
            = store.get(new Bytes(stringSerializer.serialize(null, key)), timestampTo);
        return valueAndTimestamp == null
            ? null
            : ValueAndTimestamp.make(
                stringDeserializer.deserialize(null, valueAndTimestamp.value()),
                valueAndTimestamp.timestamp());
    }

    @Test
    public void shouldPutLatest() {
        putStore("k", "v", BASE_TIMESTAMP);
        putStore("k", "v2", BASE_TIMESTAMP + 1);
        store.flush(); // ?

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest.value(), equalTo("v2"));
        assertThat(latest.timestamp(), equalTo(BASE_TIMESTAMP + 1));

        final ValueAndTimestamp<String> pastTimeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(pastTimeFilter.value(), equalTo("v"));
        assertThat(pastTimeFilter.timestamp(), equalTo(BASE_TIMESTAMP));

        final ValueAndTimestamp<String> currentTimeFilter = getFromStore("k", BASE_TIMESTAMP + 1);
        assertThat(currentTimeFilter.value(), equalTo("v2"));
        assertThat(currentTimeFilter.timestamp(), equalTo(BASE_TIMESTAMP + 1));

        final ValueAndTimestamp<String> futureTimeFilter = getFromStore("k", BASE_TIMESTAMP + 2);
        assertThat(futureTimeFilter.value(), equalTo("v2"));
        assertThat(futureTimeFilter.timestamp(), equalTo(BASE_TIMESTAMP + 1));
    }

    @Test
    public void shouldPutNullAsLatest() {
        putStore("k", null, BASE_TIMESTAMP);
        putStore("k", null, BASE_TIMESTAMP + 1);
        store.flush(); // ?

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest, nullValue());

        final ValueAndTimestamp<String> pastTimeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(pastTimeFilter, nullValue());

        final ValueAndTimestamp<String> currentTimeFilter = getFromStore("k", BASE_TIMESTAMP + 1);
        assertThat(currentTimeFilter, nullValue());

        final ValueAndTimestamp<String> futureTimeFilter = getFromStore("k", BASE_TIMESTAMP + 2);
        assertThat(futureTimeFilter, nullValue());
    }

    @Test
    public void shouldPutOlderWithNonNullLatest() {
        putStore("k", "v", BASE_TIMESTAMP);
        putStore("k", "v2", BASE_TIMESTAMP - 2);
        putStore("k", "v1", BASE_TIMESTAMP - 1);
        putStore("k", "v4", BASE_TIMESTAMP - 4);
        store.flush(); // ?

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest.value(), equalTo("v"));
        assertThat(latest.timestamp(), equalTo(BASE_TIMESTAMP));

        final ValueAndTimestamp<String> timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter.value(), equalTo("v"));
        assertThat(timeFilter.timestamp(), equalTo(BASE_TIMESTAMP));

        final ValueAndTimestamp<String> timeFilter1 = getFromStore("k", BASE_TIMESTAMP - 1);
        assertThat(timeFilter1.value(), equalTo("v1"));
        assertThat(timeFilter1.timestamp(), equalTo(BASE_TIMESTAMP - 1));

        final ValueAndTimestamp<String> timeFilter2 = getFromStore("k", BASE_TIMESTAMP - 2);
        assertThat(timeFilter2.value(), equalTo("v2"));
        assertThat(timeFilter2.timestamp(), equalTo(BASE_TIMESTAMP - 2));

        final ValueAndTimestamp<String> timeFilter3 = getFromStore("k", BASE_TIMESTAMP - 3);
        assertThat(timeFilter3.value(), equalTo("v4"));
        assertThat(timeFilter3.timestamp(), equalTo(BASE_TIMESTAMP - 4));
    }

    @Test
    public void shouldPutOlderWithNullLatest() {
        putStore("k", null, BASE_TIMESTAMP);
        putStore("k", "v2", BASE_TIMESTAMP - 2);
        putStore("k", "v1", BASE_TIMESTAMP - 1);
        putStore("k", "v4", BASE_TIMESTAMP - 4);
        store.flush(); // ?

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest, nullValue());

        final ValueAndTimestamp<String> timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter, nullValue());

        final ValueAndTimestamp<String> timeFilter1 = getFromStore("k", BASE_TIMESTAMP - 1);
        assertThat(timeFilter1.value(), equalTo("v1"));
        assertThat(timeFilter1.timestamp(), equalTo(BASE_TIMESTAMP - 1));

        final ValueAndTimestamp<String> timeFilter2 = getFromStore("k", BASE_TIMESTAMP - 2);
        assertThat(timeFilter2.value(), equalTo("v2"));
        assertThat(timeFilter2.timestamp(), equalTo(BASE_TIMESTAMP - 2));

        final ValueAndTimestamp<String> timeFilter3 = getFromStore("k", BASE_TIMESTAMP - 3);
        assertThat(timeFilter3.value(), equalTo("v4"));
        assertThat(timeFilter3.timestamp(), equalTo(BASE_TIMESTAMP - 4));
    }

    @Test
    public void shouldPutOlderNullWithNonNullLatest() {
        putStore("k", "v", BASE_TIMESTAMP);
        putStore("k", null, BASE_TIMESTAMP - 2);
        putStore("k", null, BASE_TIMESTAMP - 1);
        putStore("k", null, BASE_TIMESTAMP - 4);
        putStore("k", "v5", BASE_TIMESTAMP - 5);
        putStore("k", "v3", BASE_TIMESTAMP - 3);
        putStore("k", null, BASE_TIMESTAMP - 6);
        store.flush(); // ?

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest.value(), equalTo("v"));
        assertThat(latest.timestamp(), equalTo(BASE_TIMESTAMP));

        final ValueAndTimestamp<String> timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter.value(), equalTo("v"));
        assertThat(timeFilter.timestamp(), equalTo(BASE_TIMESTAMP));

        final ValueAndTimestamp<String> timeFilter1 = getFromStore("k", BASE_TIMESTAMP - 1);
        assertThat(timeFilter1, nullValue());

        final ValueAndTimestamp<String> timeFilter2 = getFromStore("k", BASE_TIMESTAMP - 2);
        assertThat(timeFilter2, nullValue());

        final ValueAndTimestamp<String> timeFilter4 = getFromStore("k", BASE_TIMESTAMP - 4);
        assertThat(timeFilter4, nullValue());

        final ValueAndTimestamp<String> timeFilter6 = getFromStore("k", BASE_TIMESTAMP - 6);
        assertThat(timeFilter6, nullValue());

        final ValueAndTimestamp<String> timeFilter3 = getFromStore("k", BASE_TIMESTAMP - 3);
        assertThat(timeFilter3.value(), equalTo("v3"));
        assertThat(timeFilter3.timestamp(), equalTo(BASE_TIMESTAMP - 3));

        final ValueAndTimestamp<String> timeFilter5 = getFromStore("k", BASE_TIMESTAMP - 5);
        assertThat(timeFilter5.value(), equalTo("v5"));
        assertThat(timeFilter5.timestamp(), equalTo(BASE_TIMESTAMP - 5));
    }

    @Test
    public void shouldPutOlderNullWithNullLatest() {
        putStore("k", null, BASE_TIMESTAMP);
        putStore("k", null, BASE_TIMESTAMP - 2);
        putStore("k", null, BASE_TIMESTAMP - 1);
        putStore("k", null, BASE_TIMESTAMP - 4);
        putStore("k", "v3", BASE_TIMESTAMP - 3);
        putStore("k", "v5", BASE_TIMESTAMP - 5);
        putStore("k", null, BASE_TIMESTAMP - 6);
        store.flush(); // ?

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest, nullValue());

        final ValueAndTimestamp<String> timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter, nullValue());

        final ValueAndTimestamp<String> timeFilter1 = getFromStore("k", BASE_TIMESTAMP - 1);
        assertThat(timeFilter1, nullValue());

        final ValueAndTimestamp<String> timeFilter2 = getFromStore("k", BASE_TIMESTAMP - 2);
        assertThat(timeFilter2, nullValue());

        final ValueAndTimestamp<String> timeFilter4 = getFromStore("k", BASE_TIMESTAMP - 4);
        assertThat(timeFilter4, nullValue());

        final ValueAndTimestamp<String> timeFilter6 = getFromStore("k", BASE_TIMESTAMP - 6);
        assertThat(timeFilter6, nullValue());

        final ValueAndTimestamp<String> timeFilter3 = getFromStore("k", BASE_TIMESTAMP - 3);
        assertThat(timeFilter3.value(), equalTo("v3"));
        assertThat(timeFilter3.timestamp(), equalTo(BASE_TIMESTAMP - 3));

        final ValueAndTimestamp<String> timeFilter5 = getFromStore("k", BASE_TIMESTAMP - 5);
        assertThat(timeFilter5.value(), equalTo("v5"));
        assertThat(timeFilter5.timestamp(), equalTo(BASE_TIMESTAMP - 5));
    }

    @Test
    public void shouldPutIntoMultipleSegments() {
        putStore("k", null, SEGMENT_INTERVAL - 20);
        putStore("k", "vn10", SEGMENT_INTERVAL - 10);
        putStore("k", null, SEGMENT_INTERVAL - 1);
        putStore("k", null, SEGMENT_INTERVAL + 1);
        putStore("k", "vp10", SEGMENT_INTERVAL + 10);
        putStore("k", null, SEGMENT_INTERVAL + 20);

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest, nullValue());

        final ValueAndTimestamp<String> timeFilter = getFromStore("k", SEGMENT_INTERVAL + 30);
        assertThat(timeFilter, nullValue());

        final ValueAndTimestamp<String> timeFilter1 = getFromStore("k", SEGMENT_INTERVAL + 15);
        assertThat(timeFilter1.value(), equalTo("vp10"));
        assertThat(timeFilter1.timestamp(), equalTo(SEGMENT_INTERVAL + 10));

        final ValueAndTimestamp<String> timeFilter2 = getFromStore("k", SEGMENT_INTERVAL + 5);
        assertThat(timeFilter2, nullValue());

        final ValueAndTimestamp<String> timeFilter3 = getFromStore("k", SEGMENT_INTERVAL + 2);
        assertThat(timeFilter3, nullValue());

        final ValueAndTimestamp<String> timeFilter4 = getFromStore("k", SEGMENT_INTERVAL);
        assertThat(timeFilter4, nullValue());

        final ValueAndTimestamp<String> timeFilter5 = getFromStore("k", SEGMENT_INTERVAL - 1);
        assertThat(timeFilter5, nullValue());

        final ValueAndTimestamp<String> timeFilter6 = getFromStore("k", SEGMENT_INTERVAL - 5);
        assertThat(timeFilter6.value(), equalTo("vn10"));
        assertThat(timeFilter6.timestamp(), equalTo(SEGMENT_INTERVAL - 10));

        final ValueAndTimestamp<String> timeFilter7 = getFromStore("k", SEGMENT_INTERVAL - 15);
        assertThat(timeFilter7, nullValue());
    }

    @Test
    public void shouldMoveRecordToOlderSegment() {
        putStore("k", "vp20", SEGMENT_INTERVAL + 20);
        putStore("k", "vn10", SEGMENT_INTERVAL - 10);
        putStore("k", "vn1", SEGMENT_INTERVAL - 1);
        putStore("k", "vp1", SEGMENT_INTERVAL + 1);
        putStore("k", "vp10", SEGMENT_INTERVAL + 10);

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest.value(), equalTo("vp20"));
        assertThat(latest.timestamp(), equalTo(SEGMENT_INTERVAL + 20));

        final ValueAndTimestamp<String> timeFilter = getFromStore("k", SEGMENT_INTERVAL + 30);
        assertThat(timeFilter.value(), equalTo("vp20"));
        assertThat(timeFilter.timestamp(), equalTo(SEGMENT_INTERVAL + 20));

        final ValueAndTimestamp<String> timeFilter1 = getFromStore("k", SEGMENT_INTERVAL + 15);
        assertThat(timeFilter1.value(), equalTo("vp10"));
        assertThat(timeFilter1.timestamp(), equalTo(SEGMENT_INTERVAL + 10));

        final ValueAndTimestamp<String> timeFilter2 = getFromStore("k", SEGMENT_INTERVAL + 5);
        assertThat(timeFilter2.value(), equalTo("vp1"));
        assertThat(timeFilter2.timestamp(), equalTo(SEGMENT_INTERVAL + 1));

        final ValueAndTimestamp<String> timeFilter3 = getFromStore("k", SEGMENT_INTERVAL);
        assertThat(timeFilter3.value(), equalTo("vn1"));
        assertThat(timeFilter3.timestamp(), equalTo(SEGMENT_INTERVAL - 1));

        final ValueAndTimestamp<String> timeFilter4 = getFromStore("k", SEGMENT_INTERVAL - 1);
        assertThat(timeFilter4.value(), equalTo("vn1"));
        assertThat(timeFilter4.timestamp(), equalTo(SEGMENT_INTERVAL - 1));

        final ValueAndTimestamp<String> timeFilter5 = getFromStore("k", SEGMENT_INTERVAL - 5);
        assertThat(timeFilter5.value(), equalTo("vn10"));
        assertThat(timeFilter5.timestamp(), equalTo(SEGMENT_INTERVAL - 10));
    }

    @Test
    public void shouldMoveRecordToOlderSegmentWithNulls() {
        putStore("k", null, SEGMENT_INTERVAL + 20);
        putStore("k", null, SEGMENT_INTERVAL - 10);
        putStore("k", null, SEGMENT_INTERVAL - 1);
        putStore("k", null, SEGMENT_INTERVAL + 1);
        putStore("k", null, SEGMENT_INTERVAL + 10);
        putStore("k", "vp5", SEGMENT_INTERVAL + 5);
        putStore("k", "vn5", SEGMENT_INTERVAL - 5);
        putStore("k", "vn6", SEGMENT_INTERVAL - 6);

        final ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest, nullValue());

        final ValueAndTimestamp<String> timeFilter = getFromStore("k", SEGMENT_INTERVAL + 30);
        assertThat(timeFilter, nullValue());

        final ValueAndTimestamp<String> timeFilter1 = getFromStore("k", SEGMENT_INTERVAL + 15);
        assertThat(timeFilter1, nullValue());

        final ValueAndTimestamp<String> timeFilter2 = getFromStore("k", SEGMENT_INTERVAL + 6);
        assertThat(timeFilter2.value(), equalTo("vp5"));
        assertThat(timeFilter2.timestamp(), equalTo(SEGMENT_INTERVAL + 5));

        final ValueAndTimestamp<String> timeFilter3 = getFromStore("k", SEGMENT_INTERVAL + 2);
        assertThat(timeFilter3, nullValue());

        final ValueAndTimestamp<String> timeFilter4 = getFromStore("k", SEGMENT_INTERVAL);
        assertThat(timeFilter4, nullValue());

        final ValueAndTimestamp<String> timeFilter5 = getFromStore("k", SEGMENT_INTERVAL - 1);
        assertThat(timeFilter5, nullValue());

        final ValueAndTimestamp<String> timeFilter6 = getFromStore("k", SEGMENT_INTERVAL - 5);
        assertThat(timeFilter6.value(), equalTo("vn5"));
        assertThat(timeFilter6.timestamp(), equalTo(SEGMENT_INTERVAL - 5));

        final ValueAndTimestamp<String> timeFilter7 = getFromStore("k", SEGMENT_INTERVAL - 6);
        assertThat(timeFilter7.value(), equalTo("vn6"));
        assertThat(timeFilter7.timestamp(), equalTo(SEGMENT_INTERVAL - 6));

        final ValueAndTimestamp<String> timeFilter8 = getFromStore("k", SEGMENT_INTERVAL - 8);
        assertThat(timeFilter8, nullValue());
    }

    // TODO: restore tests

    // TODO: prefix scan tests

    // TODO: cleanup tests? (ensure old records deleted)

    // TODO: tests for byte[0], empty string, and other non-null types

    // TODO: tests that repeat timestamps
}
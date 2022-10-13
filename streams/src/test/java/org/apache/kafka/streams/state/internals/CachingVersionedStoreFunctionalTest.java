package org.apache.kafka.streams.state.internals;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.streams.state.internals.VersionedStoreTestDataGeneratorUtil.computeTestCases;
import static org.apache.kafka.streams.state.internals.VersionedStoreTestDataGeneratorUtil.generateTestRecords;
import static org.apache.kafka.streams.state.internals.VersionedStoreTestDataGeneratorUtil.getGeneratedTestCaseFailureMessage;
import static org.apache.kafka.streams.state.internals.VersionedStoreTestDataGeneratorUtil.getRecordsFromFile;
import static org.apache.kafka.streams.state.internals.VersionedStoreTestDataGeneratorUtil.getSavedDataFilenames;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.internals.VersionedStoreTestDataGeneratorUtil.DataRecord;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// TODO(note): test cases from RocksDBVersionedStoreTest
public class CachingVersionedStoreFunctionalTest {

    private static final String STORE_NAME = "myversionedrocks";
    private static final String METRICS_SCOPE = "versionedrocksdb";
    private static final long HISTORY_RETENTION = 300_000L;
    private static final long SEGMENT_INTERVAL = HISTORY_RETENTION / 2;
    private static final long MAX_GENERATED_TIMESTAMP = HISTORY_RETENTION;
//    private static final long HISTORY_RETENTION = 250L;
//    private static final long SEGMENT_INTERVAL = 100L;
//    private static final long MAX_GENERATED_TIMESTAMP = 300L;

    private static final long BASE_TIMESTAMP = 10L;

    // TODO: de-dup from RocksDBStoreTest
    private final Serde<String> stringSerde = new StringSerde();
    private final Time time = new MockTime();

    protected KeyValueStoreTestDriver<String, String> driver;
    protected InternalMockProcessorContext context;
    protected VersionedKeyValueStore<String, String> store;

    @Before
    public void before() {
        // TODO(note): from AbstractKeyValueStoreTest
        driver = KeyValueStoreTestDriver.create(String.class, String.class);
        context = (InternalMockProcessorContext) driver.context();
        context.setTime(BASE_TIMESTAMP); // TODO: ?

        store = new VersionedKeyValueStoreBuilder<>(
            (RocksDbVersionedKeyValueBytesStoreSupplier) Stores.persistentVersionedKeyValueStore(STORE_NAME, Duration.ofMillis(HISTORY_RETENTION), Duration.ofMillis(SEGMENT_INTERVAL)),
            stringSerde, stringSerde, time)
            .withCachingEnabled()
            .build();
        store.init((StateStoreContext) context, store);
    }

    @After
    public void after() {
        store.close();
    }

    @Test
    public void shouldPutSavedData() {
        for (String file : getSavedDataFilenames()) {
            shouldPutSavedData(file);
            after();
            before();
        }
    }

    private void shouldPutSavedData(final String filename) {
        final String key = "k";
        final List<VersionedStoreTestDataGeneratorUtil.DataRecord> records = getRecordsFromFile(getClass(), filename);
        final Map<Long, VersionedStoreTestDataGeneratorUtil.DataRecord> testCases = computeTestCases(records);

        for (VersionedStoreTestDataGeneratorUtil.DataRecord record : records) {
            putStore(record.key, record.value, record.timestamp);
        }

        for (Map.Entry<Long, VersionedStoreTestDataGeneratorUtil.DataRecord> testCase : testCases.entrySet()) {
            final ValueAndTimestamp<String> observed = getFromStore(key, testCase.getKey());
            if (testCase.getValue().timestamp >= MAX_GENERATED_TIMESTAMP - HISTORY_RETENTION) {
                // within history retention. validate results
                if (testCase.getValue().value != null) {
                    assertThat(getGeneratedTestCaseFailureMessage(records, testCase, "Value"),
                        observed == null ? null : observed.value(), equalTo(testCase.getValue().value));
                    assertThat(getGeneratedTestCaseFailureMessage(records, testCase, "Timestamp"),
                        observed.timestamp(), equalTo(testCase.getValue().timestamp));
                } else {
                    assertThat(getGeneratedTestCaseFailureMessage(records, testCase, "Value"),
                        observed, nullValue());
                }
            }
        }
    }

    @Test
    public void shouldPutGeneratedData() {
        for (int r = 0; r < 1; r++) {
            System.out.println("r: " + r);

            final String key = "k";
            final List<VersionedStoreTestDataGeneratorUtil.DataRecord> records = generateTestRecords(MAX_GENERATED_TIMESTAMP, 1000, key);
//            final List<DataRecord> records = getRecordsFromFile(getClass(), "versioned_store_test/test_records_3.txt");
            final Map<Long, VersionedStoreTestDataGeneratorUtil.DataRecord> testCases = computeTestCases(records);

            try {
                for (VersionedStoreTestDataGeneratorUtil.DataRecord record : records) {
                    putStore(record.key, record.value, record.timestamp);
                }
            } catch (Exception e) {
                System.out.println("Failed to put data records:");
                for (VersionedStoreTestDataGeneratorUtil.DataRecord record : records) {
                    System.out.printf("\tts = %d, key = %s, value = %s%n", record.timestamp, record.key, record.value);
                }
                throw e;
            }

            for (Map.Entry<Long, VersionedStoreTestDataGeneratorUtil.DataRecord> testCase : testCases.entrySet()) {
                final ValueAndTimestamp<String> observed = getFromStore(key, testCase.getKey());
                if (testCase.getValue().timestamp >= MAX_GENERATED_TIMESTAMP - HISTORY_RETENTION) {
                    // within history retention. validate results
                    if (testCase.getValue().value != null) {
                        assertThat(getGeneratedTestCaseFailureMessage(records, testCase, "Value"),
                            observed == null ? null : observed.value(), equalTo(testCase.getValue().value));
                        assertThat(getGeneratedTestCaseFailureMessage(records, testCase, "Timestamp"),
                            observed.timestamp(), equalTo(testCase.getValue().timestamp));
                    } else {
                        assertThat(getGeneratedTestCaseFailureMessage(records, testCase, "Value"),
                            observed, nullValue());
                    }
                }
            }

            // TODO(note): hack to run the test multiple times
            after(); before();
        }
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
    public void shouldPutRepeatTimestampAsLatest() {
        putStore("k", "to_be_replaced", BASE_TIMESTAMP);
        putStore("k", "b", BASE_TIMESTAMP);

        ValueAndTimestamp<String> latest = getFromStore("k");
        assertThat(latest.value(), equalTo("b"));
        assertThat(latest.timestamp(), equalTo(BASE_TIMESTAMP));
        ValueAndTimestamp<String> timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter.value(), equalTo("b"));
        assertThat(timeFilter.timestamp(), equalTo(BASE_TIMESTAMP));

        putStore("k", null, BASE_TIMESTAMP);

        latest = getFromStore("k");
        assertThat(latest, nullValue());
        timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter, nullValue());

        putStore("k", null, BASE_TIMESTAMP);

        latest = getFromStore("k");
        assertThat(latest, nullValue());
        timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter, nullValue());

        putStore("k", "b", BASE_TIMESTAMP);

        latest = getFromStore("k");
        assertThat(latest.value(), equalTo("b"));
        assertThat(latest.timestamp(), equalTo(BASE_TIMESTAMP));
        timeFilter = getFromStore("k", BASE_TIMESTAMP);
        assertThat(timeFilter.value(), equalTo("b"));
        assertThat(timeFilter.timestamp(), equalTo(BASE_TIMESTAMP));
    }

    @Test
    public void shouldPutRepeatTimestamps() {
        putStore("k", "to_be_replaced", SEGMENT_INTERVAL + 20);
        putStore("k", null, SEGMENT_INTERVAL - 10);
        putStore("k", "to_be_replaced", SEGMENT_INTERVAL - 10);
        putStore("k", null, SEGMENT_INTERVAL - 10);
        putStore("k", "to_be_replaced", SEGMENT_INTERVAL - 1);
        putStore("k", "to_be_replaced", SEGMENT_INTERVAL + 1);
        putStore("k", null, SEGMENT_INTERVAL - 1);
        putStore("k", null, SEGMENT_INTERVAL + 1);
        putStore("k", null, SEGMENT_INTERVAL + 10);
        putStore("k", null, SEGMENT_INTERVAL + 5);
        putStore("k", "vp5", SEGMENT_INTERVAL + 5);
        putStore("k", "to_be_replaced", SEGMENT_INTERVAL - 5);
        putStore("k", "vn5", SEGMENT_INTERVAL - 5);
        putStore("k", null, SEGMENT_INTERVAL + 20);
        putStore("k", "vn6", SEGMENT_INTERVAL - 6);
        store.flush(); // ?

        verifySet4();
    }

    @Test
    public void shouldPutIntoMultipleSegments() {
        putStore("k", null, SEGMENT_INTERVAL - 20);
        putStore("k", "vn10", SEGMENT_INTERVAL - 10);
        putStore("k", null, SEGMENT_INTERVAL - 1);
        putStore("k", null, SEGMENT_INTERVAL + 1);
        putStore("k", "vp10", SEGMENT_INTERVAL + 10);
        putStore("k", null, SEGMENT_INTERVAL + 20);

        verifySet2();
    }

    @Test
    public void shouldMoveRecordToOlderSegment() {
        putStore("k", "vp20", SEGMENT_INTERVAL + 20);
        putStore("k", "vn10", SEGMENT_INTERVAL - 10);
        putStore("k", "vn1", SEGMENT_INTERVAL - 1);
        putStore("k", "vp1", SEGMENT_INTERVAL + 1);
        putStore("k", "vp10", SEGMENT_INTERVAL + 10);

        verifySet3();
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

        verifySet4();
    }

    // TODO: prefix scan tests

    // TODO: cleanup tests? (ensure old records deleted)

    // TODO: tests for byte[0], empty string, and other non-null types

    // TODO: extract inputs for these shared sets into shared code as well? (slightly annoying for the repeat tests)
    private void verifySet2() {
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

    private void verifySet3() {
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

    private void verifySet4() {
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

    private static byte[] getSerializedKey(final String key) {
        return key.getBytes(UTF_8);
    }

    private static byte[] getSerializedValue(final String value) {
        return value == null ? null : value.getBytes(UTF_8);
    }

    private void putStore(final String key, final String value, final long timestamp) {
        context.setTime(timestamp);
        store.put(
            key,
            ValueAndTimestamp.makeAllowNullable(value, timestamp)
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
        return store.get(key);
    }

    private ValueAndTimestamp<String> getFromStore(final String key, final long timestampTo) {
        return store.get(key, timestampTo);
    }

    private static class DataRecord {
        final String key;
        final String value;
        final long timestamp;
        DataRecord(String key, String value, long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    private static List<ConsumerRecord<byte[], byte[]>> getChangelogRecords(List<DataRecord> data) {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();

        for (DataRecord d : data) {
            final byte[] key = getSerializedKey(d.key);
            final byte[] value = getSerializedValue(d.value);
            records.add(new ConsumerRecord<>(
                "",
                0,
                0L,
                d.timestamp,
                TimestampType.CREATE_TIME,
                key.length,
                value == null ? 0 : value.length,
                key,
                value,
                new RecordHeaders(),
                Optional.empty()
            ));
        }

        return records;
    }
}
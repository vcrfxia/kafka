package org.apache.kafka.streams.state.internals;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class RocksDBVersionedStoreSegmentValueFormatterTest {

    private static final List<TestCase> TEST_CASES = new ArrayList<>();
    static {
        // test cases are expected to have timestamps in strictly decreasing order
        TEST_CASES.add(new TestCase("empty", 10));
        TEST_CASES.add(new TestCase("single record", 10, new TestRecord("foo".getBytes(), 1)));
        TEST_CASES.add(new TestCase("multiple records", 10, new TestRecord("foo".getBytes(), 8), new TestRecord("bar".getBytes(), 3), new TestRecord("baz".getBytes(), 0)));
        TEST_CASES.add(new TestCase("single tombstone", 10, new TestRecord(null, 1)));
        TEST_CASES.add(new TestCase("multiple tombstone", 10, new TestRecord(null, 4), new TestRecord(null, 1)));
        TEST_CASES.add(new TestCase("tombstones and records (r, t, r)", 10, new TestRecord("foo".getBytes(), 5), new TestRecord(null, 2), new TestRecord("bar".getBytes(), 1)));
        TEST_CASES.add(new TestCase("tombstones and records (t, r, t)", 10, new TestRecord(null, 5), new TestRecord("foo".getBytes(), 2), new TestRecord(null, 1)));
        TEST_CASES.add(new TestCase("tombstones and records (r, r, t, t)", 10, new TestRecord("foo".getBytes(), 6), new TestRecord("bar".getBytes(), 5), new TestRecord(null, 2), new TestRecord(null, 1)));
        TEST_CASES.add(new TestCase("tombstones and records (t, t, r, r)", 10, new TestRecord(null, 7), new TestRecord(null, 6), new TestRecord("foo".getBytes(), 2), new TestRecord("bar".getBytes(), 1)));
        TEST_CASES.add(new TestCase("record with empty bytes", 10, new TestRecord(new byte[0], 1)));
        TEST_CASES.add(new TestCase("records with empty bytes (r, e)", 10, new TestRecord("foo".getBytes(), 4), new TestRecord(new byte[0], 1)));
        TEST_CASES.add(new TestCase("records with empty bytes (e, e, r)", 10, new TestRecord(new byte[0], 8), new TestRecord(new byte[0], 2), new TestRecord("foo".getBytes(), 1)));
    }

    private final TestCase testCase;

    public RocksDBVersionedStoreSegmentValueFormatterTest(final TestCase testCase) {
        this.testCase = testCase;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestCase> data() {
        return TEST_CASES;
    }

    @Test
    public void shouldSerializeAndDeserialize() {
        final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

        final byte[] serialized = segmentValue.serialize();
        final SegmentValue deserialized = RocksDBVersionedStoreSegmentValueFormatter.deserialize(serialized);

        verifySegmentContents(deserialized, testCase);
    }

    @Test
    public void shouldBuildWithInsertLatest() {
        final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

        verifySegmentContents(segmentValue, testCase);
    }

    @Test
    public void shouldBuildWithInsertEarliest() {
        final SegmentValue segmentValue = buildSegmentWithInsertEarliest(testCase);

        verifySegmentContents(segmentValue, testCase);
    }

    @Test
    public void shouldInsertAtIndex() {
        if (testCase.records.size() == 0) {
            // cannot insert into empty segment
            return;
        }

        // test inserting at each possible index
        for (int insertIdx = 0; insertIdx <= testCase.records.size(); insertIdx++) {
            // build record to insert
            long newRecordTimestamp;
            if (insertIdx == 0) {
                newRecordTimestamp = testCase.records.get(0).timestamp + 1;
                if (newRecordTimestamp == testCase.nextTimestamp) {
                    // cannot insert because no timestamp exists between last record and nextTimestamp
                    continue;
                }
            } else {
                newRecordTimestamp = testCase.records.get(insertIdx - 1).timestamp - 1;
                if (newRecordTimestamp < 0 || (insertIdx < testCase.records.size() && newRecordTimestamp == testCase.records.get(insertIdx).timestamp)) {
                    // cannot insert because timestamps of existing records are adjacent
                    continue;
                }
            }
            TestRecord newRecord = new TestRecord("new".getBytes(), newRecordTimestamp);

            SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

            // insert() first requires a call to find()
            if (insertIdx > 0) {
                segmentValue.find(testCase.records.get(insertIdx - 1).timestamp, false);
            }
            segmentValue.insert(newRecord.timestamp, newRecord.value, insertIdx);

            // create expected results
            List<TestRecord> expectedRecords = new ArrayList<>(testCase.records);
            expectedRecords.add(insertIdx, newRecord);

            verifySegmentContents(segmentValue, new TestCase("expected", testCase.nextTimestamp, expectedRecords));
        }
    }

    @Test
    public void shouldUpdateAtIndex() {
        // test updating at each possible index
        for (int updateIdx = 0; updateIdx < testCase.records.size(); updateIdx++) {
            // build updated record
            long updatedRecordTimestamp = testCase.records.get(updateIdx).timestamp - 1;
            if (updatedRecordTimestamp < 0 || (updateIdx < testCase.records.size() - 1 && updatedRecordTimestamp == testCase.records.get(updateIdx + 1).timestamp)) {
                // found timestamp conflict. try again
                updatedRecordTimestamp = testCase.records.get(updateIdx).timestamp + 1;
                if (updateIdx > 0 && updatedRecordTimestamp == testCase.records.get(updateIdx - 1).timestamp) {
                    // found timestamp conflict. use original timestamp
                    updatedRecordTimestamp = testCase.records.get(updateIdx).timestamp;
                }
            }
            TestRecord updatedRecord = new TestRecord("updated".getBytes(), updatedRecordTimestamp);

            SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

            // updateRecord() first requires a call to find()
            segmentValue.find(testCase.records.get(updateIdx).timestamp, false);
            segmentValue.updateRecord(updatedRecord.timestamp, updatedRecord.value, updateIdx);

            // create expected results
            List<TestRecord> expectedRecords = new ArrayList<>(testCase.records);
            expectedRecords.remove(updateIdx);
            expectedRecords.add(updateIdx, updatedRecord);

            verifySegmentContents(segmentValue, new TestCase("expected", testCase.nextTimestamp, expectedRecords));
        }
    }

    @Test
    public void shouldFindByTimestamp() {
        final SegmentValue segmentValue = buildSegmentWithInsertLatest(testCase);

        // build expected mapping from timestamp -> record
        final Map<Long, Integer> expectedRecordIndices = new HashMap<>();
        for (int recordIdx = testCase.records.size() - 1; recordIdx >= 0; recordIdx--) {
            if (recordIdx < testCase.records.size() - 1) {
                expectedRecordIndices.put(testCase.records.get(recordIdx).timestamp - 1, recordIdx + 1);
            }
            if (recordIdx > 0) {
                expectedRecordIndices.put(testCase.records.get(recordIdx).timestamp + 1, recordIdx);
            }
            expectedRecordIndices.put(testCase.records.get(recordIdx).timestamp, recordIdx);
        }

        // verify results
        for (Map.Entry<Long, Integer> entry : expectedRecordIndices.entrySet()) {
            TestRecord expectedRecord = testCase.records.get(entry.getValue());
            long expectedValidTo = entry.getValue() == 0 ? testCase.nextTimestamp : testCase.records.get(entry.getValue() - 1).timestamp;

            SegmentSearchResult result = segmentValue.find(entry.getKey(), true);

            assertThat(result.includesValue(), equalTo(true));
            assertThat(result.index(), equalTo(entry.getValue()));
            assertThat(result.value(), equalTo(expectedRecord.value));
            assertThat(result.validFrom(), equalTo(expectedRecord.timestamp));
            assertThat(result.validTo(), equalTo(expectedValidTo));
        }

        // verify exception when timestamp is out of range
        final long minTimestamp = testCase.records.size() == 0 ? testCase.nextTimestamp : testCase.records.get(testCase.records.size() - 1).timestamp;
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.nextTimestamp, false));
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(testCase.nextTimestamp + 1, false));
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(minTimestamp - 1, false));
    }

    @Test
    public void shouldGetTimestamps() {
        final byte[] segmentValue = buildSegmentWithInsertLatest(testCase).serialize();
        final boolean isEmpty = testCase.records.isEmpty();

        assertThat(RocksDBVersionedStoreSegmentValueFormatter.isEmpty(segmentValue), equalTo(isEmpty));
        assertThat(RocksDBVersionedStoreSegmentValueFormatter.getNextTimestamp(segmentValue), equalTo(testCase.nextTimestamp));
        if (!isEmpty) {
            assertThat(RocksDBVersionedStoreSegmentValueFormatter.getMinTimestamp(segmentValue), equalTo(testCase.records.get(testCase.records.size() - 1).timestamp));
        }
    }

    @Test
    public void shouldCreateNewWithRecord() {
        if (testCase.records.size() != 1) {
            return;
        }

        final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(
            testCase.records.get(0).value,
            testCase.records.get(0).timestamp,
            testCase.nextTimestamp);

        verifySegmentContents(segmentValue, testCase);
    }

    private static SegmentValue buildSegmentWithInsertLatest(TestCase testCase) {
        if (testCase.records.size() == 0) {
            return RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithTombstone(testCase.nextTimestamp);
        }

        SegmentValue segmentValue = null;
        for (int recordIdx = testCase.records.size() - 1; recordIdx >= 0; recordIdx--) {
            TestRecord record = testCase.records.get(recordIdx);
            long validTo = recordIdx == 0 ? testCase.nextTimestamp : testCase.records.get(recordIdx - 1).timestamp;

            if (segmentValue == null) {
                // initialize
                if (testCase.records.size() > 1 && record.value == null) {
                    segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithTombstone(record.timestamp);
                } else {
                    segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithRecord(record.value, record.timestamp, validTo);
                }
            } else {
                // insert latest
                segmentValue.insertAsLatest(record.timestamp, validTo, record.value);
            }
        }
        return segmentValue;
    }

    private static SegmentValue buildSegmentWithInsertEarliest(TestCase testCase) {
        final SegmentValue segmentValue = RocksDBVersionedStoreSegmentValueFormatter.newSegmentValueWithTombstone(testCase.nextTimestamp);
        for (int recordIdx = 0; recordIdx < testCase.records.size(); recordIdx++) {
            TestRecord record = testCase.records.get(recordIdx);
            segmentValue.insertAsEarliest(record.timestamp, record.value);
        }
        return segmentValue;
    }

    private static void verifySegmentContents(SegmentValue segmentValue, TestCase expectedRecords) {
        // verify expected records
        for (int recordIdx = 0; recordIdx < expectedRecords.records.size(); recordIdx++) {
            TestRecord expectedRecord = expectedRecords.records.get(recordIdx);
            long expectedValidTo = recordIdx == 0 ? expectedRecords.nextTimestamp : expectedRecords.records.get(recordIdx - 1).timestamp;

            SegmentSearchResult result = segmentValue.find(expectedRecord.timestamp, true);

            assertThat(result.index(), equalTo(recordIdx));
            assertThat(result.value(), equalTo(expectedRecord.value));
            assertThat(result.validFrom(), equalTo(expectedRecord.timestamp));
            assertThat(result.validTo(), equalTo(expectedValidTo));
        }

        // verify expected exceptions to from timestamp out-of-bounds
        final long minTimestamp = expectedRecords.records.size() == 0 ? expectedRecords.nextTimestamp : expectedRecords.records.get(expectedRecords.records.size() - 1).timestamp;
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(expectedRecords.nextTimestamp, false));
        assertThrows(IllegalArgumentException.class, () -> segmentValue.find(minTimestamp - 1, false));
    }

    private static class TestRecord {
        final byte[] value;
        final long timestamp;

        TestRecord(byte[] value, long timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }
    }

    private static class TestCase {
        final List<TestRecord> records;
        final long nextTimestamp;
        final String name;

        TestCase(String name, long nextTimestamp, TestRecord... records) {
            this(name, nextTimestamp, Arrays.asList(records));
        }

        TestCase(String name, long nextTimestamp, List<TestRecord> records) {
            this.records = records;
            this.nextTimestamp = nextTimestamp;
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
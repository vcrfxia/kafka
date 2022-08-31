package org.apache.kafka.streams.state.internals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * format: <next_timestamp> + <min_timestamp> + <list of <timestamp, value_size>, reverse-sorted by timestamp> + <list of values, forward-sorted by timestamp>
 */
final class RocksDBVersionedStoreSegmentValueFormatter {
    private static final int TIMESTAMP_SIZE = 8;
    private static final int VALUE_SIZE = 4;

    static long getNextTimestamp(final byte[] segmentValue) {
        return ByteBuffer.wrap(segmentValue).getLong(0);
    }

    static boolean isEmpty(final byte[] segmentValue) {
        return segmentValue.length <= TIMESTAMP_SIZE;
    }

    // assumes not empty
    static long getMinTimestamp(final byte[] segmentValue) {
        return ByteBuffer.wrap(segmentValue).getLong(TIMESTAMP_SIZE);
    }

    static SegmentValue deserialize(final byte[] segmentValue) {
        return new PartiallyDeserializedSegmentValue(segmentValue);
    }

    static SegmentValue newSegmentValueWithRecord(
        final byte[] value, final long validFrom, final long validTo) {
        return new PartiallyDeserializedSegmentValue(value, validFrom, validTo);
    }

    static SegmentValue newSegmentValueWithTombstone(final long timestamp) {
        return new PartiallyDeserializedSegmentValue(timestamp);
    }

    interface SegmentValue {
        boolean isEmpty();

        // assumes not empty
        // assumes minTimestamp <= timestamp < nextTimestamp
        // returns record with found_ts <= timestamp < found_next_ts
        SegmentSearchResult find(long timestamp, boolean includeValue);

        // could be empty
        void insertAsLatest(long validFrom, long validTo, byte[] value);

        // could be empty
        // assumes valid insertion (timestamp < minTimestamp and insertion makes sense for segment)
        void insertAsEarliest(long timestamp, byte[] value);

        // assumes not empty
        // assumes index provided by caller is valid
        // inserts new record in list so that the new record occupies the specified index
        // assumes find() was just called on the segment, and index-1 was already deserialized
        void insert(long timestamp, byte[] value, int index);

        // assumes index provided by caller is valid (has existing record, and that this update makes sense)
        // assumes find() was just called on the segment, and this index was already deserialized
        void updateRecord(long timestamp, byte[] value, int index);

        byte[] serialize();

        class SegmentSearchResult {
            private final int index;
            private final long validFrom;
            private final long validTo;
            private final byte[] value;
            private final boolean includesValue;

            SegmentSearchResult(final int index, final long validFrom, final long validTo,
                                final byte[] value) {
                this.index = index;
                this.validFrom = validFrom;
                this.validTo = validTo;
                this.value = value;
                this.includesValue = true;
            }

            SegmentSearchResult(final int index, final long validFrom, final long validTo) {
                this.index = index;
                this.validFrom = validFrom;
                this.validTo = validTo;
                this.value = null;
                this.includesValue = false;
            }

            int index() {
                return index;
            }

            long validFrom() {
                return validFrom;
            }

            long validTo() {
                return validTo;
            }

            byte[] value() {
                return value;
            }

            boolean includesValue() {
                return includesValue;
            }
        }
    }

    private static class PartiallyDeserializedSegmentValue implements SegmentValue {
        private byte[] segmentValue;
        private long nextTimestamp;
        private long minTimestamp;
        private boolean isEmpty;

        private int deserIndex = -1; // inclusive. TODO: same as the length of the lists below minus one, until this is optimized to not eagerly truncate the lists
        private List<TimestampAndValueSize> unpackedReversedTimestampAndValueSizes;
        private List<Integer> cumulativeValueSizes; // ordered same as timestamp and value sizes (reverse time-sorted)

        private PartiallyDeserializedSegmentValue(final byte[] segmentValue) {
            this.segmentValue = segmentValue;
            this.nextTimestamp =
                RocksDBVersionedStoreSegmentValueFormatter.getNextTimestamp(segmentValue);
            this.isEmpty = RocksDBVersionedStoreSegmentValueFormatter.isEmpty(segmentValue);
            if (!isEmpty) {
                this.minTimestamp =
                    RocksDBVersionedStoreSegmentValueFormatter.getMinTimestamp(segmentValue);
            }
            resetDeserHelpers();
        }

        private PartiallyDeserializedSegmentValue(
                final byte[] valueOrNull, final long validFrom, final long validTo) {
            initializeWithRecord(translateFromNullable(valueOrNull), validFrom, validTo);
        }

        private PartiallyDeserializedSegmentValue(final long timestamp) {
            this.nextTimestamp = timestamp;
            this.segmentValue = ByteBuffer.allocate(TIMESTAMP_SIZE).putLong(nextTimestamp).array();
            this.isEmpty = true;
            resetDeserHelpers();
        }

        @Override
        public boolean isEmpty() {
            return isEmpty;
        }

        @Override
        public SegmentSearchResult find(final long timestamp, final boolean includeValue) {
            if (isEmpty || timestamp < minTimestamp || timestamp >= nextTimestamp) {
                throw new IllegalArgumentException(); // TODO: message
            }

            long currNextTimestamp = nextTimestamp;
            long currTimestamp = 0L;
            int currValueSize = 0;
            int currIndex = 0;
            int cumValueSize = 0;
            while (currTimestamp != minTimestamp) {
                if (currIndex <= deserIndex) {
                    currTimestamp = unpackedReversedTimestampAndValueSizes.get(currIndex).timestamp;
                    currValueSize = unpackedReversedTimestampAndValueSizes.get(currIndex).valueSize;
                    cumValueSize = cumulativeValueSizes.get(currIndex);
                } else {
                    int timestampSegmentIndex = 2 * TIMESTAMP_SIZE + currIndex * (TIMESTAMP_SIZE + VALUE_SIZE);
                    currTimestamp = ByteBuffer.wrap(segmentValue).getLong(timestampSegmentIndex);
                    currValueSize = ByteBuffer.wrap(segmentValue).getInt(timestampSegmentIndex + TIMESTAMP_SIZE);
                    cumValueSize += currValueSize;

                    deserIndex = currIndex;
                    unpackedReversedTimestampAndValueSizes.add(new TimestampAndValueSize(currTimestamp, currValueSize));
                    cumulativeValueSizes.add(cumValueSize);
                }

                if (currTimestamp <= timestamp) {
                    // found result
                    if (includeValue) {
                        if (currValueSize > 0) {
                            byte[] value = new byte[currValueSize];
                            int valueSegmentIndex = segmentValue.length - cumValueSize;
                            System.arraycopy(segmentValue, valueSegmentIndex, value, 0, currValueSize);
                            return new SegmentSearchResult(currIndex, currTimestamp, currNextTimestamp, value);
                        } else {
                            return new SegmentSearchResult(currIndex, currTimestamp, currNextTimestamp, null);
                        }
                    } else {
                        return new SegmentSearchResult(currIndex, currTimestamp, currNextTimestamp);
                    }
                }

                // prep for next iteration
                currNextTimestamp = currTimestamp;
                currIndex++;
            }

            throw new IllegalStateException("Search in segment expected to find result but did not.");
        }

        @Override
        public void insertAsLatest(final long validFrom, final long validTo, final byte[] valueOrNull) {
            final byte[] value = translateFromNullable(valueOrNull);

            if (nextTimestamp > validFrom) {
                // detected inconsistency edge case where older segment has [a,b) while newer store
                // has [a,c), due to [b,c) having failed to write to newer store.
                // remove entries from this store until the overlap is resolved. TODO: details
                throw new UnsupportedOperationException("case not yet implemented");
            }

            if (nextTimestamp != validFrom) {
                // move nextTimestamp into list as tombstone and add new record on top
                if (isEmpty) {
                    initializeWithRecord(new byte[0], nextTimestamp, validFrom);
                } else {
                    insert(nextTimestamp, new byte[0], 0);
                }
                insert(validFrom, value, 0); // TODO: make more efficient by combining two insert()'s into one?
            } else {
                // nextTimestamp is moved into segment automatically as record is added on top
                if (isEmpty) {
                    initializeWithRecord(value, validFrom, validTo);
                } else {
                    insert(validFrom, value, 0);
                }
            }
            // update nextTimestamp. TODO: make more efficient by removing second pass?
            nextTimestamp = validTo;
            ByteBuffer.wrap(segmentValue, 0, TIMESTAMP_SIZE).putLong(nextTimestamp); // TODO: is this putLong() correct, given the subarray wrapping before it?
        }

        @Override
        public void insertAsEarliest(final long timestamp, final byte[] valueOrNull) {
            final byte[] value = translateFromNullable(valueOrNull);

            if (isEmpty) {
                initializeWithRecord(value, timestamp, nextTimestamp);
            } else {
                int lastIndex = find(minTimestamp, false).index;
                insert(timestamp, value, lastIndex + 1);
            }
        }

        @Override
        public void insert(final long timestamp, final byte[] valueOrNull, final int index) { // TODO: return type?
            if (isEmpty || index > deserIndex + 1 || index < 0) {
                throw new IllegalArgumentException("Must invoke find() to deserialize record before insert() at specific index.");
            }
            final byte[] value = translateFromNullable(valueOrNull);

            final boolean needsMinTsUpdate = isLastIndex(index - 1); // TODO(note): check first before updating info below
            truncateDeserHelpersToIndex(index - 1);
            unpackedReversedTimestampAndValueSizes.add(new TimestampAndValueSize(timestamp, value.length));
            int prevCumValueSize = deserIndex == -1 ? 0 : cumulativeValueSizes.get(deserIndex);
            cumulativeValueSizes.add(prevCumValueSize + value.length);
            deserIndex++;

            // update serialization and other props
            int segmentTimestampIndex = 2 * TIMESTAMP_SIZE + index * (TIMESTAMP_SIZE + VALUE_SIZE);
            segmentValue = ByteBuffer.allocate(segmentValue.length + TIMESTAMP_SIZE + VALUE_SIZE + value.length)
                .put(segmentValue, 0, segmentTimestampIndex)
                .putLong(timestamp)
                .putInt(value.length)
                .put(segmentValue, segmentTimestampIndex, segmentValue.length - segmentTimestampIndex - prevCumValueSize)
                .put(value)
                .put(segmentValue, segmentValue.length - prevCumValueSize, prevCumValueSize)
                .array();

            if (needsMinTsUpdate) {
                if (minTimestamp <= timestamp) { // TODO: remove
                    throw new IllegalStateException("insert as earliest has timestamp later than existing min timestamp");
                }

                minTimestamp = timestamp;
                ByteBuffer.wrap(segmentValue, TIMESTAMP_SIZE, TIMESTAMP_SIZE).putLong(TIMESTAMP_SIZE, minTimestamp); // TODO: is this putLong() with index usage correct, given the subarray wrapping before it?
            }
        }

        @Override
        public void updateRecord(final long timestamp, final byte[] valueOrNull, final int index) {
            if (index > deserIndex || index < 0) {
                throw new IllegalArgumentException("Must invoke find() to deserialize record before updateRecord().");
            }
            final byte[] value = translateFromNullable(valueOrNull);

            int oldValueSize = unpackedReversedTimestampAndValueSizes.get(index).valueSize;
            int oldCumValueSize = cumulativeValueSizes.get(index);

            final boolean needsMinTsUpdate = isLastIndex(index); // TODO(note): check first before updating info below
            unpackedReversedTimestampAndValueSizes.set(index, new TimestampAndValueSize(timestamp, value.length));
            cumulativeValueSizes.set(index, oldCumValueSize - oldValueSize + value.length);
            truncateDeserHelpersToIndex(index); // TODO: technically only need to purge cumulativeValueSizes and not unpackedReversedTimestampAndValueSizes

            // update serialization and other props
            int segmentTimestampIndex = 2 * TIMESTAMP_SIZE + index * (TIMESTAMP_SIZE + VALUE_SIZE);
            segmentValue = ByteBuffer.allocate(segmentValue.length - oldValueSize + value.length)
            .put(segmentValue, 0, segmentTimestampIndex)
            .putLong(timestamp)
            .putInt(value.length)
            .put(segmentValue, segmentTimestampIndex + TIMESTAMP_SIZE + VALUE_SIZE, segmentValue.length - (segmentTimestampIndex + TIMESTAMP_SIZE + VALUE_SIZE) - oldCumValueSize)
            .put(value)
            .put(segmentValue, segmentValue.length - oldCumValueSize + oldValueSize, oldCumValueSize - oldValueSize)
            .array();

            if (needsMinTsUpdate) {
                minTimestamp = timestamp;
                ByteBuffer.wrap(segmentValue, TIMESTAMP_SIZE, TIMESTAMP_SIZE).putLong(TIMESTAMP_SIZE, minTimestamp); // TODO: is this putLong() with index usage correct, given the subarray wrapping before it?
            }
        }

        @Override
        public byte[] serialize() {
            return segmentValue;
        }

        private void initializeWithRecord(final byte[] value, final long validFrom, final long validTo) {
            if (validFrom >= validTo) { // TODO: remove
                throw new IllegalStateException("validFrom must be less than validTo");
            }

            this.nextTimestamp = validTo;
            this.minTimestamp = validFrom;
            this.segmentValue = ByteBuffer.allocate(TIMESTAMP_SIZE * 3 + VALUE_SIZE + value.length)
                .putLong(nextTimestamp)
                .putLong(minTimestamp)
                .putLong(validFrom)
                .putInt(value.length)
                .put(value)
                .array();
            this.isEmpty = false;
            resetDeserHelpers();
        }

        private void resetDeserHelpers() {
            deserIndex = -1;
            unpackedReversedTimestampAndValueSizes = new ArrayList<>(); // TODO: consider initializing to null instead
            cumulativeValueSizes = new ArrayList<>();
        }

        // TODO: verify behavior, including for edge case where index = -1
        private void truncateDeserHelpersToIndex(final int index) {
            deserIndex = index;
            unpackedReversedTimestampAndValueSizes.subList(index + 1, unpackedReversedTimestampAndValueSizes.size()).clear(); // TODO: check
            cumulativeValueSizes.subList(index + 1, cumulativeValueSizes.size()).clear();
        }

        private boolean isLastIndex(final int index) {
            if (deserIndex == -1
                || unpackedReversedTimestampAndValueSizes.get(deserIndex).timestamp != minTimestamp) {
                return false;
            }
            return index == deserIndex;
        }

        // TODO: this needs to be updated if we want to distinguish between null and byte[0]
        private static byte[] translateFromNullable(final byte[] valueOrNull) {
            return valueOrNull == null ? new byte[0] : valueOrNull;
        }

        private static class TimestampAndValueSize {
            final long timestamp;
            final int valueSize;

            TimestampAndValueSize(final long timestamp, final int valueSize) {
                this.timestamp = timestamp;
                this.valueSize = valueSize;
            }
        }
    }
}

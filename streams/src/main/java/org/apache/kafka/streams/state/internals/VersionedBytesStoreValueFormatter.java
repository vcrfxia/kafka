package org.apache.kafka.streams.state.internals;

import java.nio.ByteBuffer;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedRecord;

final class VersionedBytesStoreValueFormatter {

    // static helpers for converting to/from byte array format:
    // - format is <timestamp> + <bool indicating whether value is a tombstone> + <raw value>
    // - the bool is needed in order to distinguish between tombstone and empty value (i.e., byte[0])
    // - the bool is stored as 8 bytes for now for convenience, but would be trimmed down in the real implementation

    static byte[] rawValue(final byte[] rawValueAndTimestamp) {
        final int rawValueLength = rawValueAndTimestamp.length - 16;
        final byte[] value = new byte[rawValueLength];
        System.arraycopy(rawValueAndTimestamp, 16, value, 0, rawValueLength);
        return value;
    }

    static long timestamp(final byte[] rawValueAndTimestamp) {
        return ByteBuffer
            .wrap(rawValueAndTimestamp)
            .getLong(0);
    }

    static boolean isTombstone(final byte[] rawValueAndTimestamp) {
        final long tombstoneIndicator = ByteBuffer
            .wrap(rawValueAndTimestamp)
            .getLong(8);
        return tombstoneIndicator == 1;
    }

    static byte[] toReturnBytes(final VersionedRecord<byte[]> versionedRecord) {
        if (versionedRecord == null) {
            return null;
        }
        return ByteBuffer
            .allocate(versionedRecord.value().length + 16)
            .putLong(versionedRecord.timestamp())
            .putLong(0L)
            .put(versionedRecord.value())
            .array();
    }

    static byte[] toPutBytes(final ValueAndTimestamp<byte[]> valueAndTimestamp) {
        // valueAndTimestamp will never be null
        if (valueAndTimestamp.value() != null) {
            return ByteBuffer
                .allocate(valueAndTimestamp.value().length + 16)
                .putLong(valueAndTimestamp.timestamp())
                .putLong(0L)
                .put(valueAndTimestamp.value())
                .array();
        } else {
            return ByteBuffer
                .allocate(16)
                .putLong(valueAndTimestamp.timestamp())
                .putLong(1L)
                .array();
        }
    }
}

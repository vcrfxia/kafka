package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.RecordBatchingStateRestoreCallback;
import org.apache.kafka.streams.processor.internals.StoreToProcessorContextAdapter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.processor.internals.metrics.TaskMetrics;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBVersionedStore implements VersionedKeyValueStore<Bytes, byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBVersionedStore.class);
    private static final long SENTINEL_TIMESTAMP = -1L;

    private final String name;
    private final long historyRetention;
    private final RocksDBMetricsRecorder metricsRecorder;

    private final RocksDBStore latestValueStore;
    private final KeyValueSegments segmentStores;
    private final LatestValueSchema latestValueSchema;
    private final SegmentValueSchema segmentValueSchema;

    private ProcessorContext context;
    private StateStoreContext stateStoreContext;
    private Sensor expiredRecordSensor;
    private long observedStreamTime = ConsumerRecord.NO_TIMESTAMP;
    private boolean consistencyEnabled = false;
    private Position position;
    private OffsetCheckpoint positionCheckpoint;
    private volatile boolean open;

    RocksDBVersionedStore(final String name, final String metricsScope, final long historyRetention, final long segmentInterval) {
        this.name = name;
        this.historyRetention = historyRetention;
        this.metricsRecorder = new RocksDBMetricsRecorder(metricsScope, name);
        this.latestValueStore = new RocksDBStore(lvsName(name), "rocksdb", metricsRecorder); // TODO: dir name probably isn't right?
        this.segmentStores = new KeyValueSegments(segmentsName(name), historyRetention, segmentInterval, metricsRecorder);
        this.latestValueSchema = new LatestValueSchema();
        this.segmentValueSchema = new SegmentValueSchema();
    }

    // valueAndTimestamp should never come in as null, should always be a null wrapped with a timestamp
    @Override
    public void put(final Bytes key, final ValueAndTimestamp<byte[]> valueAndTimestamp) {
        // TODO: complicated logic here. see AbstractDualSchemaRocksDBSegmentedBytesStore for inspiration
        final long timestamp = valueAndTimestamp.timestamp();
        observedStreamTime = Math.max(observedStreamTime, timestamp);

        // check latest value store
        long foundTs = SENTINEL_TIMESTAMP; // tracks smallest timestamp larger than insertion timestamp seen so far
        final byte[] latestValue = latestValueStore.get(key);
        if (latestValue != null) {
            foundTs = latestValueSchema.getTimestamp(latestValue);
            if (timestamp >= foundTs) {
                // move existing latest value into segment
                final long segmentId = segmentStores.segmentId(timestamp);
                final KeyValueSegment segment = segmentStores.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
                if (segment == null) {
                    LOG.debug("Not moving existing latest value to segment for old update.");
                } else {
                    final byte[] foundValue = latestValueSchema.getValue(latestValue);
                    final byte[] segmentValueBytes = segment.get(key);
                    if (segmentValueBytes == null) {
                        segment.put(
                            key,
                            segmentValueSchema
                                .newSegmentValueWithRecord(foundValue, foundTs, timestamp)
                                .serialize()
                        );
                    } else {
                        final SegmentValue segmentValue = segmentValueSchema.deserialize(segmentValueBytes);
                        segmentValue.insertAsLatest(foundTs, timestamp, foundValue);
                        segment.put(key, segmentValue.serialize());
                    }
                }

                // update latest value store
                if (valueAndTimestamp.value() != null) {
                    latestValueStore.put(key, latestValueSchema.from(valueAndTimestamp.value(), timestamp));
                }
                return;
            }
        }

        // continue search in segments
        final List<KeyValueSegment> segments = segmentStores.segments(timestamp, Long.MAX_VALUE, false);
        for (final KeyValueSegment segment : segments) {
            final byte[] segmentValue = segment.get(key);
            if (segmentValue != null) {
                final long foundNextTs = segmentValueSchema.getNextTimestamp(segmentValue);
                if (foundNextTs <= timestamp) { // TODO: this shouldn't actually happen because we filtered for segments based on timestamp
                    // this segment (and all earlier segments) does not contain records affected by
                    // this put. insert into the tentativeSegmentId and conclude the procedure.
                    // (same procedure as "insert into tentative segment" below, and then break)
                    throw new IllegalStateException("should not find segment out of range for timestamp");
                }

                if (segmentValueSchema.isEmpty(segmentValue)) {
                    // the record being inserted belongs in this segment.
                    // insert and conclude the procedure.
                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    sv.insertAsEarliest(timestamp, valueAndTimestamp.value());
                    segment.put(key, sv.serialize());
                    break;
                }

                final long minFoundTs = segmentValueSchema.getMinTimestamp(segmentValue);
                if (minFoundTs <= timestamp) {
                    // the record being inserted belongs in this segment.
                    // insert and conclude the procedure.
                    final long segmentIdForTimestamp = segmentStores.segmentId(timestamp);
                    final boolean writeToOlderSegmentNeeded = segmentIdForTimestamp != segment.id;

                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    final SegmentSearchResult searchResult = sv.find(timestamp, writeToOlderSegmentNeeded);

                    if (writeToOlderSegmentNeeded) {
                        // existing record needs to be moved to an older segment. do this first.
                        final KeyValueSegment olderSegment = segmentStores
                            .getOrCreateSegmentIfLive(segmentIdForTimestamp, context, observedStreamTime);
                        final byte[] olderSegmentValue = olderSegment.get(key);
                        if (olderSegmentValue == null) {
                            olderSegment.put(
                                key,
                                segmentValueSchema.newSegmentValueWithRecord(
                                    searchResult.value(), searchResult.validFrom(), timestamp
                                ).serialize()
                            );
                        } else {
                            final SegmentValue olderSv = segmentValueSchema.deserialize(olderSegmentValue);
                            olderSv.insertAsLatest(searchResult.validFrom(), timestamp, searchResult.value());
                            olderSegment.put(key, olderSv.serialize());
                        }

                        // update in newer segment (replace the record that was just moved with the new one)
                        sv.updateRecord(timestamp, valueAndTimestamp.value(), searchResult.index());
                        segment.put(key, sv.serialize());
                    } else {
                        sv.insert(timestamp, valueAndTimestamp.value(), searchResult.index());
                        segment.put(key, sv.serialize());
                    }
                    break;
                }

                // TODO: we can technically remove this
                if (minFoundTs < observedStreamTime - historyRetention) {
                    // the record being inserted does not affect version history. discard and return
                    LOG.warn("Skipping record for expired put.");
                    return;
                }

                // it's possible the record belongs in this segment, but also possible it belongs
                // in an earlier segment. mark as tentative and continue.
                foundTs = minFoundTs;

                // TODO: we could skip past some segments according to minFoundTs. add this optimization later
            }
        }

        // ran out of segments to search. insert into tentative segment.
        if (foundTs == SENTINEL_TIMESTAMP) {
            // insert into latest value store
            if (valueAndTimestamp.value() != null) {
                latestValueStore.put(key, latestValueSchema.from(valueAndTimestamp.value(), timestamp));
            } else {
                // tombstones are not inserted into the latest value store. insert into segment instead
                final KeyValueSegment segment = segmentStores.getOrCreateSegmentIfLive(
                    segmentStores.segmentId(timestamp), context, observedStreamTime);
                if (segment == null) {
                    LOG.warn("Skipping record for expired put.");
                    return;
                }

                final byte[] segmentValue = segment.get(key);
                if (segmentValue == null) {
                    segment.put(key, segmentValueSchema.newSegmentValueWithTombstone(timestamp).serialize());
                } else {
                    // insert as latest, since foundTs = sentinel means nothing later exists
                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    sv.insertAsLatest(
                        segmentValueSchema.getNextTimestamp(segmentValue),
                        timestamp,
                        null // TODO: hopefully this is fine, rather than byte[0]?
                    );
                    segment.put(key, sv.serialize());
                }
            }
        } else {
            // insert into segment corresponding to foundTs. the new record is the earliest in this
            // segment, or the segment is empty (in which case the new record could be either before
            // or after the existing tombstone)
            final KeyValueSegment segment = segmentStores.getOrCreateSegmentIfLive(
                segmentStores.segmentId(foundTs), context, observedStreamTime);
            if (segment == null) {
                LOG.warn("Skipping record for expired put.");
                return;
            }

            final byte[] segmentValue = segment.get(key);
            if (segmentValue == null) {
                segment.put(key, segmentValueSchema.newSegmentValueWithRecord(
                    valueAndTimestamp.value(), timestamp, foundTs
                ).serialize());
            } else {
                if (segmentValueSchema.isEmpty(segmentValue)
                    && segmentValueSchema.getNextTimestamp(segmentValue) < timestamp) {
                    // insert as latest into empty segment
                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    sv.insertAsLatest(
                        timestamp,
                        foundTs,
                        valueAndTimestamp.value()
                    );
                    segment.put(key, sv.serialize());
                } else {
                    if (!segmentValueSchema.isEmpty(segmentValue)
                        && segmentValueSchema.getMinTimestamp(segmentValue) > timestamp) {
                        throw new IllegalStateException(
                            "Incorrect assumption about fall-through insertion always being earliest");
                    }

                    // insert as earliest (into possibly empty segment)
                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    sv.insertAsEarliest(timestamp, valueAndTimestamp.value());
                    segment.put(key, sv.serialize());
                }
            }
        }
    }

    @Override
    public ValueAndTimestamp<byte[]> putIfAbsent(final Bytes key, final ValueAndTimestamp<byte[]> value) { // TODO: where is this called from? do we actually need this?
        // TODO: segmented stores don't have this (comes from KeyValueStore)
        return null;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, ValueAndTimestamp<byte[]>>> entries) {
        // TODO: segmented stores don't have this (comes from KeyValueStore)
    }

    @Override
    public ValueAndTimestamp<byte[]> delete(final Bytes key) { // TODO: where is this called from? do we actually need the return value here?
        // TODO: segmented store equivalent of this (comes from KeyValueStore) is remove()
        put(key, ValueAndTimestamp.makeAllowNullable(null, context.timestamp()));
        return null;
    }

    @Override
    public ValueAndTimestamp<byte[]> get(final Bytes key) {
        // latest value is guaranteed to be present in the latest value store
        final byte[] latestValue = latestValueStore.get(key);
        if (latestValue != null) {
            return ValueAndTimestamp.make(
                latestValueSchema.getValue(latestValue),
                latestValueSchema.getTimestamp(latestValue)
            );
        } else {
            return null;
        }
    }

    @Override
    public ValueAndTimestamp<byte[]> get(final Bytes key, final long timestampTo) {
        // TODO: see AbstractDualSchemaRocksDBSegmentedBytesStore for inspiration
        // first check the latest value store
        final byte[] latestValue = latestValueStore.get(key);
        if (latestValue != null) {
            final long latestTimestamp = latestValueSchema.getTimestamp(latestValue);
            if (latestTimestamp <= timestampTo) {
                return ValueAndTimestamp
                    .make(latestValueSchema.getValue(latestValue), latestTimestamp);
            }
        }

        // check segment stores
        final List<KeyValueSegment> segments = segmentStores.segments(timestampTo, Long.MAX_VALUE, false);
        for (final KeyValueSegment segment : segments) {
            final byte[] segmentValue = segment.get(key);
            if (segmentValue != null) {
                final long nextTs = segmentValueSchema.getNextTimestamp(segmentValue);
                if (nextTs <= timestampTo) {
                    // this segment contains no data for the queried timestamp, so earlier segments
                    // cannot either
                    return null;
                }

                if (segmentValueSchema.isEmpty(segmentValue)
                    || segmentValueSchema.getMinTimestamp(segmentValue) > timestampTo) {
                    // the segment only contains data for after the queried timestamp. skip and
                    // continue the search to earlier segments.
                    continue;
                    // TODO: add optimization to jump forward in segments based on minFoundTs?
                }

                // the desired result is contained in this segment
                final SegmentSearchResult searchResult =
                    segmentValueSchema.deserialize(segmentValue).find(timestampTo, true);
                if (searchResult.value() != null) { // TODO: handle byte[0] here?
                    return ValueAndTimestamp.make(searchResult.value(), searchResult.validFrom());
                } else {
                    return null;
                }
            }
        }

        // checked all segments and no results found
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(final Bytes from, final Bytes to) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(final Bytes from, final Bytes to, final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(final Bytes from, final Bytes to) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(final Bytes from, final Bytes to, final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all() {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all(final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll() {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll(final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public void deleteHistory(final long timestampTo) { // TODO: where is deleteHistory called from?
        // TODO: do we actually need to call explicit cleanup on segments?
        // cleanup is already called implicitly whenever getOrCreateSegmentIfLive() is called,
        // by using the stream time passed in with the call
        // looks like getOrCreateSegmentIfLive() isn't called consistently on put() so it's
        // probably better to add an explicit call rather than rely on this. might make sense to
        // add a call into put() rather than have a separate method here. might also be good to
        // remove the implicit cleanup in AbstractSegments in order to avoid overhead
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void flush() {
        segmentStores.flush();
        latestValueStore.flush(); // TODO: inconsistency concern if second flush fails?
    }

    @Override
    public void close() {
        open = false;
        latestValueStore.close(); // TODO: inconsistency concern with regards to order?
        segmentStores.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public Position getPosition() {
        return position;
    }

    @Override
    public long approximateNumEntries() {
        // TODO: decide what to do with this. comes from ReadOnlyKeyValueStore
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        // TODO(note): copied from AbstractDualSchemaRocksDBSegmentedBytesStore
        this.context = context;

        final StreamsMetricsImpl metrics = ProcessorContextUtils.getMetricsImpl(context);
        final String threadId = Thread.currentThread().getName();
        final String taskName = context.taskId().toString();

        expiredRecordSensor = TaskMetrics.droppedRecordsSensor(
            threadId,
            taskName,
            metrics
        );

        metricsRecorder.init(ProcessorContextUtils.getMetricsImpl(context), context.taskId()); // TODO: where does this need to go? came from KeyValueSegments#openExisting()

        latestValueStore.openDB(context.appConfigs(), context.stateDir()); // TODO: check
        //latestValueStore.init(context, root);
        segmentStores.openExisting(context, observedStreamTime);

        final File positionCheckpointFile = new File(context.stateDir(), name() + ".position");
        this.positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        this.position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);

        // register and possibly restore the state from the logs
        stateStoreContext.register(
            root,
            (RecordBatchingStateRestoreCallback) this::restoreAllInternal,
            () -> StoreQueryUtils.checkpointPosition(positionCheckpoint, position)
        );

        open = true;

        consistencyEnabled = StreamsConfig.InternalConfig.getBoolean(
            context.appConfigs(),
            IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED,
            false
        );
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        this.stateStoreContext = context;
        init(StoreToProcessorContextAdapter.adapt(context), root);
    }

    private void restoreAllInternal(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        // TODO: this is a problem. requires reading from db before it is open, which is not allowed today
    }

    // TODO: convert to interface, move elsewhere, unify implementation with elsewhere?
    // bytes layout: timestamp + value
    private static class LatestValueSchema {
        private static final int TIMESTAMP_SIZE = 8;
        long getTimestamp(final byte[] latestValue) {
            return ByteBuffer.wrap(latestValue).getLong();
        }
        byte[] getValue(final byte[] latestValue) {
            // TODO: bypass this array copy?
            byte[] value = new byte[latestValue.length - TIMESTAMP_SIZE];
            System.arraycopy(latestValue, TIMESTAMP_SIZE, value, 0, value.length);
            return value;
        }
        byte[] from(final byte[] value, final long timestamp) {
            if (value == null) {
                throw new IllegalArgumentException("Should not store tombstone in latest value");
            }

            return ByteBuffer.allocate(TIMESTAMP_SIZE + value.length)
                .putLong(timestamp)
                .put(value)
                .array();
        }
    }

    // TODO: convert to interface, move elsewhere, have RocksDBVersionedStoreSegmentValueFormatter
    // implement interface
    private static class SegmentValueSchema {
        long getNextTimestamp(final byte[] segmentValue) {
            return RocksDBVersionedStoreSegmentValueFormatter.getNextTimestamp(segmentValue);
        }
        boolean isEmpty(final byte[] segmentValue) {
            return RocksDBVersionedStoreSegmentValueFormatter.isEmpty(segmentValue);
        }
        long getMinTimestamp(final byte[] segmentValue) {
            return RocksDBVersionedStoreSegmentValueFormatter.getMinTimestamp(segmentValue);
        }
        SegmentValue deserialize(final byte[] segmentValue) {
            return RocksDBVersionedStoreSegmentValueFormatter.deserialize(segmentValue);
        }
        SegmentValue newSegmentValueWithRecord(
            final byte[] value, final long validFrom, final long validTo) {
            return RocksDBVersionedStoreSegmentValueFormatter
                .newSegmentValueWithRecord(value, validFrom, validTo);
        }
        SegmentValue newSegmentValueWithTombstone(final long timestamp) {
            return RocksDBVersionedStoreSegmentValueFormatter
                .newSegmentValueWithTombstone(timestamp);
        }
    }

    private static String lvsName(final String storeName) {
        return storeName + ".latestValues"; // TODO: verify delimiter
    }

    private static String segmentsName(final String storeName) {
        return storeName + ".segments"; // TODO: verify delimiter
    }
}

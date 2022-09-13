package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
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
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue;
import org.apache.kafka.streams.state.internals.RocksDBVersionedStoreSegmentValueFormatter.SegmentValue.SegmentSearchResult;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBVersionedStore implements CacheableVersionedKeyValueStore<Bytes, byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBVersionedStore.class);
    private static final long SENTINEL_TIMESTAMP = -1L;

    private final String name;
    private final long historyRetention;
    private final RocksDBMetricsRecorder metricsRecorder;

    private final RocksDBStore latestValueStore;
    private final KeyValueSegments segmentStores;
    private final LatestValueSchema latestValueSchema;
    private final SegmentValueSchema segmentValueSchema;
    private final VersionedStoreClient<KeyValueSegment> versionedStoreClient;

    private final RocksDBVersionedStoreRestoreHelper restoreHelper;
    private final VersionedStoreClient<Long> versionedStoreRestoreClient;

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
        this.versionedStoreClient = new RocksDBVersionedStoreClient();
        this.restoreHelper = RocksDBVersionedStoreRestoreHelper.makeWithRemovalListener(
            (k, v) -> latestValueStore.put(k, v, true),
            (segmentId, k, v) -> {
                final KeyValueSegment segment = segmentStores.getOrCreateSegment(segmentId, context); // TODO: update to IfLive once processor time is taken into account
                segment.put(k, v, true);
            }
        );
        this.versionedStoreRestoreClient = restoreHelper.getRestoreClient(versionedStoreClient, segmentStores::segmentId); // TODO: convert store client to read-only
    }

    // valueAndTimestamp should never come in as null, should always be a null wrapped with a timestamp
    @Override
    public void put(final Bytes key, final ValueAndTimestamp<byte[]> valueAndTimestamp) {
        observedStreamTime = putHelper(
            latestValueSchema,
            segmentValueSchema,
            versionedStoreClient,
            segmentStores::segmentId,
            context,
            observedStreamTime,
            historyRetention,
            key,
            valueAndTimestamp
        );
    }

    @Override
    public ValueAndTimestamp<byte[]> putIfAbsent(final Bytes key, final ValueAndTimestamp<byte[]> valueAndTimestamp) { // TODO: where is this called from? do we actually need this?
        LOG.info(String.format("vxia debug: putIfAbsent: key (%s), value (%s), ts (%d)",
            key.toString(),
            valueAndTimestamp.value() == null ? "null" : Arrays.toString(valueAndTimestamp.value()),
            valueAndTimestamp.timestamp()
        ));

        // TODO: segmented stores don't have this (comes from KeyValueStore)
        return null;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, ValueAndTimestamp<byte[]>>> entries) {
        LOG.info("vxia debug: putAll:");
        for (final KeyValue<Bytes, ValueAndTimestamp<byte[]>> entry : entries) {
            LOG.info(String.format("vxia debug: \tkey (%s), value (%s), ts (%d)",
                entry.key.toString(),
                entry.value.value() == null ? "null" : Arrays.toString(entry.value.value()),
                entry.value.timestamp()
            ));
        }

        // TODO: segmented stores don't have this (comes from KeyValueStore)
    }

    @Override
    public ValueAndTimestamp<byte[]> delete(final Bytes key) { // TODO: where is this called from? do we actually need the return value here?
        LOG.info(String.format("vxia debug: delete: key (%s), ts (%d)",
            key.toString(),
            context.timestamp()
        ));

        // TODO: segmented store equivalent of this (comes from KeyValueStore) is remove()
        put(key, ValueAndTimestamp.makeAllowNullable(null, context.timestamp()));
        return null;
    }

    @Override
    public ValueAndTimestamp<byte[]> get(final Bytes key) {
        LOG.info(String.format("vxia debug: get: key (%s)",
            key.toString()
        ));

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
        LOG.info(String.format("vxia debug: get: key (%s), tsTo (%d)",
            key.toString(),
            timestampTo
        ));

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
        LOG.info(String.format("vxia debug: range: from (%s), to (%s)",
            from.toString(),
            to.toString()
        ));

        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> range(final Bytes from, final Bytes to, final long timestampTo) {
        LOG.info(String.format("vxia debug: range: from (%s), to (%s), tsTo (%d)",
            from.toString(),
            to.toString(),
            timestampTo
        ));

        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(final Bytes from, final Bytes to) {
        LOG.info(String.format("vxia debug: reverseRange: from (%s), to (%s)",
            from.toString(),
            to.toString()
        ));

        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseRange(final Bytes from, final Bytes to, final long timestampTo) {
        LOG.info(String.format("vxia debug: reverseRange: from (%s), to (%s), tsTo (%d)",
            from.toString(),
            to.toString(),
            timestampTo
        ));

        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all() {
        LOG.info("vxia debug: all");

        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> all(final long timestampTo) {
        LOG.info(String.format("vxia debug: all: tsTo (%d)",
            timestampTo
        ));

        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll() {
        LOG.info("vxia debug: reverseAll");

        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> reverseAll(final long timestampTo) {
        LOG.info(String.format("vxia debug: reverseAll: tsTo (%d)",
            timestampTo
        ));

        // TODO
        return null;
    }

    @Override
    public void deleteHistory(final long timestampTo) { // TODO: where is deleteHistory called from?
        LOG.info(String.format("vxia debug: deleteHistory: tsTo (%d)",
            timestampTo
        ));

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
        LOG.info("vxia debug: flush");

        segmentStores.flush();
        latestValueStore.flush(); // TODO: inconsistency concern if second flush fails?
    }

    @Override
    public void close() {
        LOG.info("vxia debug: close");

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
        LOG.info("vxia debug: init");

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

        latestValueStore.openDB(context.appConfigs(), context.stateDir()); // TODO: check -- does this mean we actually don't need to relax the restriction that restore is allowed to take place before db is open?
        //latestValueStore.init(context, root);
        segmentStores.openExisting(context, observedStreamTime);

        final File positionCheckpointFile = new File(context.stateDir(), name() + ".position");
        this.positionCheckpoint = new OffsetCheckpoint(positionCheckpointFile);
        this.position = StoreQueryUtils.readPositionFromCheckpoint(positionCheckpoint);

        // register and possibly restore the state from the logs
        stateStoreContext.register(
            root,
            new RecordBatchingStateRestoreCallback() {
                @Override
                public void restoreBatch(Collection<ConsumerRecord<byte[], byte[]>> records) {
                    RocksDBVersionedStore.this.restoreBatch(records);
                }

                @Override
                public void finishRestore() {
                    RocksDBVersionedStore.this.finishRestore();
                }
            },
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

    // VisibleForTesting
    void restoreBatch(final Collection<ConsumerRecord<byte[], byte[]>> records) {
        for (ConsumerRecord<byte[], byte[]> record : records) {
            putHelper(
                latestValueSchema,
                segmentValueSchema,
                versionedStoreRestoreClient,
                segmentStores::segmentId, // TODO: extract into variable?
                context,
                observedStreamTime,
                historyRetention,
                new Bytes(record.key()),
                ValueAndTimestamp.makeAllowNullable(record.value(), record.timestamp())
            );
        }
    }

    // VisibleForTesting
    void finishRestore() {
        restoreHelper.flushAll();
    }

    @Override
    public void replaceFromCache(final Bytes key, final ValueAndTimestamp<byte[]> value, long nextTimestamp) {
        // put bypassing latest value, update next timestamp
        // TODO
    }

    @Override
    public void bypassCache(final Bytes key, final ValueAndTimestamp<byte[]> value, final long nextTimestamp) {
        // put bypassing latest value, next timestamp should not need an update
        // TODO
    }

    @Override
    public void newKeyInsertedToCache(final Bytes key, long nextTimestamp) {
        // move latest value to segment, update next timestamp in the process
        // TODO
    }

    interface VersionedStoreClient<T> {
        default byte[] getLatestValue(Bytes key) {
            return getLatestValue(key, false);
        }
        byte[] getLatestValue(Bytes key, boolean isRestoring);

        void putLatestValue(Bytes key, byte[] value);
        void deleteLatestValue(Bytes key);

        T getOrCreateSegmentIfLive(long segmentId, ProcessorContext context, long streamTime);
        List<T> getReverseSegments(long timestampFrom, Bytes key);

        default byte[] getFromSegment(T segment, Bytes key) {
            return getFromSegment(segment, key, false);
        }
        byte[] getFromSegment(T segment, Bytes key, boolean isRestoring);

        void putToSegment(T segment, Bytes key, byte[] value);

        long getIdForSegment(T segment);
        T getSegmentIfPresent(long segmentId); // TODO(note): hack to allow cache client to delegate getFromSegment() to db client
    }

    private final class RocksDBVersionedStoreClient implements VersionedStoreClient<KeyValueSegment> {

        @Override
        public byte[] getLatestValue(Bytes key, boolean isRestoring) {
            return latestValueStore.get(key, isRestoring);
        }

        @Override
        public void putLatestValue(Bytes key, byte[] value) {
            latestValueStore.put(key, value);
        }

        @Override
        public void deleteLatestValue(Bytes key) {
            latestValueStore.delete(key);
        }

        @Override
        public KeyValueSegment getOrCreateSegmentIfLive(long segmentId, ProcessorContext context, long streamTime) {
            return segmentStores.getOrCreateSegmentIfLive(segmentId, context, streamTime);
        }

        @Override
        public List<KeyValueSegment> getReverseSegments(long timestampFrom, Bytes key) {
            // do not attempt filter by key because it is slow. return all segments instead.
            return segmentStores.segments(timestampFrom, Long.MAX_VALUE, false);
        }

        @Override
        public byte[] getFromSegment(KeyValueSegment segment, Bytes key, boolean isRestoring) {
            return segment.get(key, isRestoring);
        }

        @Override
        public void putToSegment(KeyValueSegment segment, Bytes key, byte[] value) {
            segment.put(key, value);
        }

        @Override
        public long getIdForSegment(KeyValueSegment segment) {
            return segment.id;
        }

        @Override
        public KeyValueSegment getSegmentIfPresent(long segmentId) {
            return segmentStores.getSegment(segmentId);
        }
    }

    // returns new stream time
    static <T> long putHelper(
        final LatestValueSchema latestValueSchema,
        final SegmentValueSchema segmentValueSchema,
        final VersionedStoreClient<T> versionedStoreClient,
        final Function<Long, Long> segmentIdGetter,
        final ProcessorContext context,
        final long observedStreamTime,
        final long historyRetention,
        final Bytes key,
        final ValueAndTimestamp<byte[]> valueAndTimestamp
    ) {
        LOG.info(String.format("vxia debug: put: key (%s), value (%s), ts (%d)",
            key.toString(),
            valueAndTimestamp.value() == null ? "null" : Arrays.toString(valueAndTimestamp.value()),
            valueAndTimestamp.timestamp()
        ));

        // TODO: complicated logic here. see AbstractDualSchemaRocksDBSegmentedBytesStore for inspiration
        final long timestamp = valueAndTimestamp.timestamp();
        final long newStreamTime = Math.max(observedStreamTime, timestamp);

        // check latest value store
        long foundTs = SENTINEL_TIMESTAMP; // tracks smallest timestamp larger than insertion timestamp seen so far
        final byte[] latestValue = versionedStoreClient.getLatestValue(key);
        if (latestValue != null) {
            foundTs = latestValueSchema.getTimestamp(latestValue);
            if (timestamp >= foundTs) {
                // move existing latest value into segment
                final long segmentId = segmentIdGetter.apply(timestamp);
                final T segment = versionedStoreClient.getOrCreateSegmentIfLive(segmentId, context, observedStreamTime);
                if (segment == null) {
                    LOG.info("vxia debug: Not moving existing latest value to segment for old update.");
                } else {
                    final byte[] foundValue = latestValueSchema.getValue(latestValue);
                    final byte[] segmentValueBytes = versionedStoreClient.getFromSegment(segment, key);
                    if (segmentValueBytes == null) {
                        versionedStoreClient.putToSegment(
                            segment,
                            key,
                            segmentValueSchema
                                .newSegmentValueWithRecord(foundValue, foundTs, timestamp)
                                .serialize()
                        );
                    } else {
                        final SegmentValue segmentValue = segmentValueSchema.deserialize(segmentValueBytes);
                        segmentValue.insertAsLatest(foundTs, timestamp, foundValue);
                        versionedStoreClient.putToSegment(segment, key, segmentValue.serialize());
                    }
                }

                // update latest value store
                if (valueAndTimestamp.value() != null) {
                    versionedStoreClient.putLatestValue(key, latestValueSchema.from(valueAndTimestamp.value(), timestamp));
                } else {
                    versionedStoreClient.deleteLatestValue(key);
                }
                return newStreamTime;
            }
        }

        // continue search in segments
        final List<T> segments = versionedStoreClient.getReverseSegments(timestamp, key);
        for (final T segment : segments) {
            final byte[] segmentValue = versionedStoreClient.getFromSegment(segment, key);
            if (segmentValue != null) {
                final long foundNextTs = segmentValueSchema.getNextTimestamp(segmentValue);
                if (foundNextTs <= timestamp) {
                    // this segment (and all earlier segments) does not contain records affected by
                    // this put. insert into the tentativeSegmentId and conclude the procedure.
                    // (break to use same procedure "insert into tentative segment" as below)
                    break;
                }

                if (segmentValueSchema.isEmpty(segmentValue)) {
                    // the record being inserted belongs in this segment.
                    // insert and conclude the procedure.
                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    sv.insertAsEarliest(timestamp, valueAndTimestamp.value());
                    versionedStoreClient.putToSegment(segment, key, sv.serialize());
                    return newStreamTime;
                }

                final long minFoundTs = segmentValueSchema.getMinTimestamp(segmentValue);
                if (minFoundTs <= timestamp) {
                    // the record being inserted belongs in this segment.
                    // insert and conclude the procedure.
                    final long segmentIdForTimestamp = segmentIdGetter.apply(timestamp);
                    final boolean writeToOlderSegmentNeeded
                        = segmentIdForTimestamp != versionedStoreClient.getIdForSegment(segment);

                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    final SegmentSearchResult searchResult = sv.find(timestamp, writeToOlderSegmentNeeded);

                    if (writeToOlderSegmentNeeded) {
                        // existing record needs to be moved to an older segment. do this first.
                        final T olderSegment = versionedStoreClient
                            .getOrCreateSegmentIfLive(segmentIdForTimestamp, context, observedStreamTime);
                        final byte[] olderSegmentValue = versionedStoreClient.getFromSegment(olderSegment, key);
                        if (olderSegmentValue == null) {
                            versionedStoreClient.putToSegment(
                                olderSegment,
                                key,
                                segmentValueSchema.newSegmentValueWithRecord(
                                    searchResult.value(), searchResult.validFrom(), timestamp
                                ).serialize()
                            );
                        } else {
                            final SegmentValue olderSv = segmentValueSchema.deserialize(olderSegmentValue);
                            olderSv.insertAsLatest(searchResult.validFrom(), timestamp, searchResult.value());
                            versionedStoreClient.putToSegment(olderSegment, key, olderSv.serialize());
                        }

                        // update in newer segment (replace the record that was just moved with the new one)
                        sv.updateRecord(timestamp, valueAndTimestamp.value(), searchResult.index());
                        versionedStoreClient.putToSegment(segment, key, sv.serialize());
                    } else {
                        sv.insert(timestamp, valueAndTimestamp.value(), searchResult.index());
                        versionedStoreClient.putToSegment(segment, key, sv.serialize());
                    }
                    return newStreamTime;
                }

                // TODO: we can technically remove this
                if (minFoundTs < observedStreamTime - historyRetention) {
                    // the record being inserted does not affect version history. discard and return
                    LOG.warn("Skipping record for expired put.");
                    return newStreamTime;
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
                versionedStoreClient.putLatestValue(key, latestValueSchema.from(valueAndTimestamp.value(), timestamp));
            } else {
                // tombstones are not inserted into the latest value store. insert into segment instead
                final T segment = versionedStoreClient.getOrCreateSegmentIfLive(
                    segmentIdGetter.apply(timestamp), context, observedStreamTime);
                if (segment == null) {
                    LOG.warn("Skipping record for expired put.");
                    return newStreamTime;
                }

                final byte[] segmentValue = versionedStoreClient.getFromSegment(segment, key);
                if (segmentValue == null) {
                    versionedStoreClient.putToSegment(segment, key, segmentValueSchema.newSegmentValueWithTombstone(timestamp).serialize());
                } else {
                    // insert as latest, since foundTs = sentinel means nothing later exists
                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    sv.insertAsLatest(
                        segmentValueSchema.getNextTimestamp(segmentValue),
                        timestamp,
                        null
                    );
                    versionedStoreClient.putToSegment(segment, key, sv.serialize());
                }
            }
        } else {
            // insert into segment corresponding to foundTs. the new record is the earliest in this
            // segment, or the segment is empty (in which case the new record could be either before
            // or after the existing tombstone)
            final T segment = versionedStoreClient.getOrCreateSegmentIfLive(
                segmentIdGetter.apply(foundTs), context, observedStreamTime);
            if (segment == null) {
                LOG.warn("Skipping record for expired put.");
                return newStreamTime;
            }

            final byte[] segmentValue = versionedStoreClient.getFromSegment(segment, key);
            if (segmentValue == null) {
                if (valueAndTimestamp.value() != null) {
                    versionedStoreClient.putToSegment(segment, key, segmentValueSchema.newSegmentValueWithRecord(
                        valueAndTimestamp.value(), timestamp, foundTs
                    ).serialize());
                } else {
                    versionedStoreClient.putToSegment(
                        segment,
                        key,
                        segmentValueSchema.newSegmentValueWithTombstone(timestamp).serialize()
                    );
                }
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
                    versionedStoreClient.putToSegment(segment, key, sv.serialize());
                } else {
                    if (!segmentValueSchema.isEmpty(segmentValue)
                        && segmentValueSchema.getMinTimestamp(segmentValue) < timestamp) { // TODO: what about equality case? general issue that needs to be patched throughout the code
                        throw new IllegalStateException(
                            "Incorrect assumption about fall-through insertion always being earliest");
                    }

                    // insert as earliest (into possibly empty segment)
                    final SegmentValue sv = segmentValueSchema.deserialize(segmentValue);
                    sv.insertAsEarliest(timestamp, valueAndTimestamp.value());
                    versionedStoreClient.putToSegment(segment, key, sv.serialize());
                }
            }
        }
        return newStreamTime;
    }

    // TODO: convert to interface, move elsewhere, unify implementation with elsewhere?
    // bytes layout: timestamp + value
    static class LatestValueSchema {
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
    static class SegmentValueSchema {
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

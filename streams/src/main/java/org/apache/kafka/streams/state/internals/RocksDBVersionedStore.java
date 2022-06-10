package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.streams.StreamsConfig.InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED;

import java.io.File;
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
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBVersionedStore implements VersionedKeyValueStore<Bytes, byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBVersionedStore.class);

    private final String name;
    private final long historyRetention;

    private final RocksDBStore latestValueStore;
    private final KeyValueSegments segments;
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
        this.latestValueStore = new RocksDBStore(lvsName(name), metricsScope);
        this.segments = new KeyValueSegments(segmentsName(name), metricsScope, historyRetention, segmentInterval);
        this.latestValueSchema = new LatestValueSchema();
        this.segmentValueSchema = new SegmentValueSchema();
    }

    @Override
    public void put(final Bytes key, final byte[] value) { // TODO: interface needs timestamp
        // TODO: complicated logic here. see AbstractDualSchemaRocksDBSegmentedBytesStore for inspiration
    }

    @Override
    public byte[] putIfAbsent(final Bytes key, final byte[] value) {
        // TODO: segmented stores don't have this (comes from KeyValueStore)
        return null;
    }

    @Override
    public void putAll(final List<KeyValue<Bytes, byte[]>> entries) {
        // TODO: segmented stores don't have this (comes from KeyValueStore)
    }

    @Override
    public byte[] delete(final Bytes key) {
        // TODO: segmented store equivalent of this (comes from KeyValueStore) is remove()
        return null;
    }

    @Override
    public byte[] get(final Bytes key) {
        // TODO: see AbstractDualSchemaRocksDBSegmentedBytesStore for inspiration
        return null;
    }

    @Override
    public byte[] get(final Bytes key, final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> range(final Bytes from, final Bytes to, final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseRange(final Bytes from, final Bytes to, final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all() {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> all(final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll() {
        // TODO
        return null;
    }

    @Override
    public KeyValueIterator<Bytes, byte[]> reverseAll(final long timestampTo) {
        // TODO
        return null;
    }

    @Override
    public void deleteHistory(final long timestampTo) {
        // TODO
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void flush() {
        segments.flush();
        latestValueStore.flush(); // TODO: inconsistency concern if second flush fails?
    }

    @Override
    public void close() {
        open = false;
        latestValueStore.close(); // TODO: inconsistency concern with regards to order?
        segments.close();
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

        segments.openExisting(context, observedStreamTime);

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
        // TODO: this is a problem. requires reading from db before it is open, which
        // is not allowed today
    }

    private static class LatestValueSchema {
        // TODO
    }

    private static class SegmentValueSchema {
        // TODO
    }

    private static String lvsName(final String storeName) {
        return storeName + ".latestValues"; // TODO: verify delimiter
    }

    private static String segmentsName(final String storeName) {
        return storeName + ".segments"; // TODO: verify delimiter
    }
}

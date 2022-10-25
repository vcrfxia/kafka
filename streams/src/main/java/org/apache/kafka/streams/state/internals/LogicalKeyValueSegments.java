package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

public class LogicalKeyValueSegments extends AbstractSegments<LogicalKeyValueSegment> {

    private final RocksDBMetricsRecorder metricsRecorder;
    private final RocksDBStore physicalStore;

    // VisibleForTesting
    LogicalKeyValueSegments(final String name,
                     final String parentDir,
                     final String metricsScope,
                     final long retentionPeriod,
                     final long segmentInterval) {
        this(name, parentDir, retentionPeriod, segmentInterval,
            new RocksDBMetricsRecorder(metricsScope, name));
    }

    LogicalKeyValueSegments(final String name,
                     final String parentDir,
                     final long retentionPeriod,
                     final long segmentInterval,
                     final RocksDBMetricsRecorder metricsRecorder) {
        super(name, retentionPeriod, segmentInterval);
        this.metricsRecorder = metricsRecorder;
        this.physicalStore = new RocksDBStore(name, parentDir, metricsRecorder, true);
    }

    // TODO(note): ugly hack to allow RocksDBVersionedStoreRestoreClient to delegate getFromSegment() to RocksDBVersionedStoreClient
    public LogicalKeyValueSegment getSegment(final long segmentId) {
        return segments.get(segmentId);
    }


    @Override
    public LogicalKeyValueSegment getOrCreateSegment(final long segmentId,
                                                     final ProcessorContext context) {
        if (segments.containsKey(segmentId)) {
            return segments.get(segmentId);
        } else {
            final LogicalKeyValueSegment newSegment = new LogicalKeyValueSegment(segmentId, segmentName(segmentId), physicalStore);

            if (segments.put(segmentId, newSegment) != null) {
                throw new IllegalStateException("KeyValueSegment already exists. Possible concurrent access.");
            }

            newSegment.openDB();
            return newSegment;
        }
    }

    @Override
    public void openExisting(final ProcessorContext context, final long streamTime) {
        metricsRecorder.init(ProcessorContextUtils.getMetricsImpl(context), context.taskId()); // TODO: where does the equivalent of this need to go, for the latest value store in RocksDBVersionedStore?
        // initialize this physical store
        physicalStore.openDB(context.appConfigs(), context.stateDir());

        // there used to be a call to
        // super.openExisting(context, streamTime);
        // here but I don't think it's needed anymore: looks like it checks the state dir to
        // call getOrCreateSegment() based on existing dirs, which used to be important because
        // that's when the physical rocksdbs would be opened, but for the logicals we're using it's
        // a no-op
    }

    @Override
    public void flush() {
        // flush the physical store. no need to flush logicals
        physicalStore.flush();
    }

    @Override
    public void close() {
        super.close(); // close segments first

        physicalStore.close(); // then close the physical store
    }
}

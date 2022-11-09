/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

/**
 * Manages the {@link KeyValueSegment}s that are used by the {@link RocksDBSegmentedBytesStore}
 */
class KeyValueSegments extends AbstractSegments<KeyValueSegment> {

    private final RocksDBMetricsRecorder metricsRecorder;

    KeyValueSegments(final String name,
                     final String metricsScope,
                     final long retentionPeriod,
                     final long segmentInterval) {
        this(name, retentionPeriod, segmentInterval,
            new RocksDBMetricsRecorder(metricsScope, name));
    }

    KeyValueSegments(final String name,
                     final long retentionPeriod,
                     final long segmentInterval,
                     final RocksDBMetricsRecorder metricsRecorder) {
        super(name, retentionPeriod, segmentInterval);
        this.metricsRecorder = metricsRecorder;
    }

    // TODO(note): ugly hack to allow RocksDBCacheEnabledVersionedStoreRestoreClient to delegate getFromSegment() to RocksDBVersionedStoreClient
    public KeyValueSegment getSegment(final long segmentId) {
        return segments.get(segmentId);
    }

    @Override
    public KeyValueSegment getOrCreateSegment(final long segmentId,
                                              final ProcessorContext context) {
        if (segments.containsKey(segmentId)) {
            return segments.get(segmentId);
        } else {
            final KeyValueSegment newSegment =
                new KeyValueSegment(segmentName(segmentId), name, segmentId, metricsRecorder);

            if (segments.put(segmentId, newSegment) != null) {
                throw new IllegalStateException("KeyValueSegment already exists. Possible concurrent access.");
            }

            newSegment.openDB(context.appConfigs(), context.stateDir()); // TODO: what are these configs?
            return newSegment;
        }
    }

    @Override
    public void openExisting(final ProcessorContext context, final long streamTime) {
        metricsRecorder.init(ProcessorContextUtils.getMetricsImpl(context), context.taskId()); // TODO: where does the equivalent of this need to go, for the latest value store in RocksDBVersionedStore?
        super.openExisting(context, streamTime);
    }
}
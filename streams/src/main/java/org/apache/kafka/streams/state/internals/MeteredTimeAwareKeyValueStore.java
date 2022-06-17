package org.apache.kafka.streams.state.internals;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareKeySerde;
import static org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl.maybeMeasureLatency;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.kstream.internals.Change;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.StoreQueryUtils.QueryHandler;
import org.apache.kafka.streams.state.internals.metrics.StateStoreMetrics;

// TODO: de-dup from MeteredKeyValueStore
public class MeteredTimeAwareKeyValueStore<K, V>
    extends WrappedStateStore<TimestampedKeyValueStore<Bytes, byte[]>, K, ValueAndTimestamp<V>>
    implements TimestampedKeyValueStore<K, V> {

    final Serde<K> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<K, V> serdes;

    private final String metricsScope;
    protected final Time time;
    protected Sensor putSensor;
    private Sensor putIfAbsentSensor;
    protected Sensor getSensor;
    private Sensor deleteSensor;
    private Sensor putAllSensor;
    private Sensor allSensor;
    private Sensor rangeSensor;
    private Sensor prefixScanSensor;
    private Sensor flushSensor;
    private Sensor e2eLatencySensor;
    private InternalProcessorContext context;
    private StreamsMetricsImpl streamsMetrics;
    private TaskId taskId;

    @SuppressWarnings("rawtypes")
    private final Map<Class, QueryHandler> queryHandlers =
        mkMap(
            mkEntry(
                RangeQuery.class,
                (query, positionBound, config, store) -> runRangeQuery(query, positionBound, config)
            ),
            mkEntry(
                KeyQuery.class,
                (query, positionBound, config, store) -> runKeyQuery(query, positionBound, config)
            )
        );

    MeteredTimeAwareKeyValueStore(final TimestampedKeyValueStore<Bytes, byte[]> inner,
                                  final String metricsScope,
                                  final Time time,
                                  final Serde<K> keySerde,
                                  final Serde<V> valueSerde) {
        super(inner);
        this.metricsScope = metricsScope;
        this.time = time != null ? time : Time.SYSTEM;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context,
                     final StateStore root) {
        this.context = context instanceof InternalProcessorContext ? (InternalProcessorContext) context : null;
        taskId = context.taskId();
        initStoreSerde(context);
        streamsMetrics = (StreamsMetricsImpl) context.metrics();

        registerMetrics();
        final Sensor restoreSensor =
            StateStoreMetrics.restoreSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        // register and possibly restore the state from the logs
        maybeMeasureLatency(() -> super.init(context, root), time, restoreSensor);
    }

    @Override
    public void init(final StateStoreContext context,
                     final StateStore root) {
        this.context = context instanceof InternalProcessorContext ? (InternalProcessorContext<?, ?>) context : null;
        taskId = context.taskId();
        initStoreSerde(context);
        streamsMetrics = (StreamsMetricsImpl) context.metrics();

        registerMetrics();
        final Sensor restoreSensor =
            StateStoreMetrics.restoreSensor(taskId.toString(), metricsScope, name(), streamsMetrics);

        // register and possibly restore the state from the logs
        maybeMeasureLatency(() -> super.init(context, root), time, restoreSensor);
    }

    private void registerMetrics() {
        putSensor = StateStoreMetrics.putSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        putIfAbsentSensor = StateStoreMetrics.putIfAbsentSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        putAllSensor = StateStoreMetrics.putAllSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        getSensor = StateStoreMetrics.getSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        allSensor = StateStoreMetrics.allSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        rangeSensor = StateStoreMetrics.rangeSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        prefixScanSensor = StateStoreMetrics.prefixScanSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        flushSensor = StateStoreMetrics.flushSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        deleteSensor = StateStoreMetrics.deleteSensor(taskId.toString(), metricsScope, name(), streamsMetrics);
        e2eLatencySensor = StateStoreMetrics.e2ELatencySensor(taskId.toString(), metricsScope, name(), streamsMetrics);
    }

    protected Serde<V> prepareValueSerdeForStore(final Serde<V> valueSerde, final SerdeGetter getter) {
        return WrappingNullableUtils.prepareValueSerde(valueSerde, getter);
    }

    @Deprecated
    private void initStoreSerde(final ProcessorContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
        serdes = new StateSerdes<>(
            changelogTopic,
            prepareKeySerde(keySerde, new SerdeGetter(context)),
            prepareValueSerdeForStore(valueSerde, new SerdeGetter(context))
        );
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName, Boolean.FALSE);
        serdes = new StateSerdes<>(
            changelogTopic,
            prepareKeySerde(keySerde, new SerdeGetter(context)),
            prepareValueSerdeForStore(valueSerde, new SerdeGetter(context))
        );
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean setFlushListener(final CacheFlushListener<K, ValueAndTimestamp<V>> listener,
                                    final boolean sendOldValues) {
        final TimestampedKeyValueStore<Bytes, byte[]> wrapped = wrapped();
        if (wrapped instanceof CachedStateStore) {
            return ((CachedStateStore<byte[], ValueAndTimestamp<byte[]>>) wrapped).setFlushListener( // TODO: why is key here byte[] rather than Bytes?
                record -> listener.apply(
                    record.withKey(serdes.keyFrom(record.key()))
                        .withValue(new Change<>(
                            ValueAndTimestamp.make(record.value().newValue.value() != null ? serdes.valueFrom(record.value().newValue.value()) : null, record.value().newValue.timestamp()), // TODO: assumes that nulls are still wrapped in ValueAndTimestamp with relevant timestamp. verify
                            ValueAndTimestamp.make(record.value().oldValue.value() != null ? serdes.valueFrom(record.value().oldValue.value()) : null, record.value().oldValue.timestamp())
                        ))
                ),
                sendOldValues);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <R> QueryResult<R> query(final Query<R> query,
                                    final PositionBound positionBound,
                                    final QueryConfig config) {

        final long start = time.nanoseconds();
        final QueryResult<R> result;

        final QueryHandler handler = queryHandlers.get(query.getClass());
        if (handler == null) {
            result = wrapped().query(query, positionBound, config);
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + getClass() + " in " + (time.nanoseconds() - start) + "ns");
            }
        } else {
            result = (QueryResult<R>) handler.apply(
                query,
                positionBound,
                config,
                this
            );
            if (config.isCollectExecutionInfo()) {
                result.addExecutionInfo(
                    "Handled in " + getClass() + " with serdes "
                        + serdes + " in " + (time.nanoseconds() - start) + "ns");
            }
        }
        return result;
    }

    @Override
    public Position getPosition() {
        return wrapped().getPosition();
    }

    private <R> QueryResult<R> runRangeQuery(final Query<R> query,
                                             final PositionBound positionBound,
                                             final QueryConfig config) {
        return null; // TODO
    }

    private <R> QueryResult<R> runKeyQuery(final Query<R> query,
                                           final PositionBound positionBound,
                                           final QueryConfig config) {
        return null; // TODO
    }

    @Override
    public ValueAndTimestamp<V> get(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            return maybeMeasureLatency(() -> outerValue(wrapped().get(keyBytes(key))), time, getSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public void put(final K key,
                    final ValueAndTimestamp<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            maybeMeasureLatency(() -> wrapped().put(keyBytes(key), innerValue(value)), time, putSensor);
            maybeRecordE2ELatency();
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key, value);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(final K key,
                         final ValueAndTimestamp<V> value) {
        Objects.requireNonNull(key, "key cannot be null");
        final ValueAndTimestamp<V> currentValue = maybeMeasureLatency(
            () -> outerValue(wrapped().putIfAbsent(keyBytes(key), innerValue(value))),
            time,
            putIfAbsentSensor
        );
        maybeRecordE2ELatency();
        return currentValue;
    }

    @Override
    public void putAll(final List<KeyValue<K, ValueAndTimestamp<V>>> entries) {
        entries.forEach(entry -> Objects.requireNonNull(entry.key, "key cannot be null"));
        maybeMeasureLatency(() -> wrapped().putAll(innerEntries(entries)), time, putAllSensor);
    }

    @Override
    public ValueAndTimestamp<V> delete(final K key) {
        Objects.requireNonNull(key, "key cannot be null");
        try {
            return maybeMeasureLatency(() -> outerValue(wrapped().delete(keyBytes(key))), time, deleteSensor);
        } catch (final ProcessorStateException e) {
            final String message = String.format(e.getMessage(), key);
            throw new ProcessorStateException(message, e);
        }
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueAndTimestamp<V>> prefixScan(final P prefix, final PS prefixKeySerializer) {
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(prefixKeySerializer, "prefixKeySerializer cannot be null");
        return new MeteredTimeAwareKeyValueIterator(wrapped().prefixScan(prefix, prefixKeySerializer), prefixScanSensor);
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(final K from,
                                        final K to) {
        final byte[] serFrom = from == null ? null : serdes.rawKey(from);
        final byte[] serTo = to == null ? null : serdes.rawKey(to);
        return new MeteredTimeAwareKeyValueIterator(
            wrapped().range(Bytes.wrap(serFrom), Bytes.wrap(serTo)),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseRange(final K from,
                                               final K to) {
        final byte[] serFrom = from == null ? null : serdes.rawKey(from);
        final byte[] serTo = to == null ? null : serdes.rawKey(to);
        return new MeteredTimeAwareKeyValueIterator(
            wrapped().reverseRange(Bytes.wrap(serFrom), Bytes.wrap(serTo)),
            rangeSensor
        );
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        return new MeteredTimeAwareKeyValueIterator(wrapped().all(), allSensor);
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseAll() {
        return new MeteredTimeAwareKeyValueIterator(wrapped().reverseAll(), allSensor);
    }

    @Override
    public void flush() {
        maybeMeasureLatency(super::flush, time, flushSensor);
    }

    @Override
    public long approximateNumEntries() {
        return wrapped().approximateNumEntries();
    }

    @Override
    public void close() {
        try {
            wrapped().close();
        } finally {
            streamsMetrics.removeAllStoreLevelSensorsAndMetrics(taskId.toString(), name());
        }
    }

    protected ValueAndTimestamp<V> outerValue(final ValueAndTimestamp<byte[]> value) {
        return value != null
            ? ValueAndTimestamp.make(serdes.valueFrom(value.value()), value.timestamp())
            : null;
    }

    protected ValueAndTimestamp<byte[]> innerValue(final ValueAndTimestamp<V> value) {
        // TODO: don't think this needs a null check, verify.
        return ValueAndTimestamp.make(serdes.rawValue(value.value()), value.timestamp());
    }

    protected Bytes keyBytes(final K key) {
        return Bytes.wrap(serdes.rawKey(key));
    }

    private List<KeyValue<Bytes, ValueAndTimestamp<byte[]>>> innerEntries(final List<KeyValue<K, ValueAndTimestamp<V>>> from) {
        final List<KeyValue<Bytes, ValueAndTimestamp<byte[]>>> byteEntries = new ArrayList<>();
        for (final KeyValue<K, ValueAndTimestamp<V>> entry : from) {
            byteEntries.add(KeyValue.pair(Bytes.wrap(serdes.rawKey(entry.key)), innerValue(entry.value)));
        }
        return byteEntries;
    }

    private void maybeRecordE2ELatency() {
        // Context is null if the provided context isn't an implementation of InternalProcessorContext.
        // In that case, we _can't_ get the current timestamp, so we don't record anything.
        if (e2eLatencySensor.shouldRecord() && context != null) {
            final long currentTime = time.milliseconds();
            final long e2eLatency =  currentTime - context.timestamp();
            e2eLatencySensor.record(e2eLatency, currentTime);
        }
    }

    private class MeteredTimeAwareKeyValueIterator implements KeyValueIterator<K, ValueAndTimestamp<V>> {

        private final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> iter;
        private final Sensor sensor;
        private final long startNs;

        private MeteredTimeAwareKeyValueIterator(final KeyValueIterator<Bytes, ValueAndTimestamp<byte[]>> iter,
                                        final Sensor sensor) {
            this.iter = iter;
            this.sensor = sensor;
            this.startNs = time.nanoseconds();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<K, ValueAndTimestamp<V>> next() {
            final KeyValue<Bytes, ValueAndTimestamp<byte[]>> keyValue = iter.next();
            return KeyValue.pair(
                serdes.keyFrom(keyValue.key.get()),
                outerValue(keyValue.value));
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                sensor.record(time.nanoseconds() - startNs);
            }
        }

        @Override
        public K peekNextKey() {
            return serdes.keyFrom(iter.peekNextKey().get());
        }
    }
}

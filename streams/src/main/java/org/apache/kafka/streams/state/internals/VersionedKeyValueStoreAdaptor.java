package org.apache.kafka.streams.state.internals;

import java.util.List;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.Query;
import org.apache.kafka.streams.query.QueryConfig;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;
import org.apache.kafka.streams.state.VersionedKeyValueStoreInternal;

class VersionedKeyValueStoreAdaptor<K, V> implements VersionedKeyValueStoreInternal<K, V> {

    private final VersionedKeyValueStore<K, V> inner;

    VersionedKeyValueStoreAdaptor(final VersionedKeyValueStore<K, V> inner) {
        this.inner = inner;
    }

    @Override
    public void put(K key, ValueAndTimestamp<V> valueAndTimestamp) {
        // TODO: assert value not null (and similar for other methods)
        inner.put(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp());
    }

    @Override
    public ValueAndTimestamp<V> putIfAbsent(K key, ValueAndTimestamp<V> valueAndTimestamp) {
        return inner.putIfAbsent(key, valueAndTimestamp.value(), valueAndTimestamp.timestamp());;
    }

    @Override
    public void putAll(List<KeyValue<K, ValueAndTimestamp<V>>> entries) {
        // TODO: conversion
    }

    @Override
    public ValueAndTimestamp<V> delete(K key) {
        inner.delete(key);
        return null; // TODO: this return type is a problem
    }

    @Override
    public ValueAndTimestamp<V> get(K key) {
        return inner.get(key);
    }

    @Override
    public ValueAndTimestamp<V> get(K key, long timestampTo) {
        return inner.get(key, timestampTo);
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(K from, K to) {
        // TODO: conversion
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> range(K from, K to, long timestampTo) {
        // TODO: conversion
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseRange(K from, K to) {
        // TODO: conversion
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseRange(K from, K to, long timestampTo) {
        // TODO: conversion
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> all() {
        // TODO: conversion
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> all(long timestampTo) {
        // TODO: conversion
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseAll() {
        // TODO: conversion
        return null;
    }

    @Override
    public KeyValueIterator<K, ValueAndTimestamp<V>> reverseAll(long timestampTo) {
        // TODO: conversion
        return null;
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        inner.init(context, root);
    }

    @Override
    public void flush() {
        inner.flush();
    }

    @Override
    public void close() {
        inner.close();
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public boolean isOpen() {
        return inner.isOpen();
    }

    @Override
    public <R> QueryResult<R> query(Query<R> query, PositionBound positionBound, QueryConfig config) {
        return inner.query(query, positionBound, config);
    }

    @Override
    public Position getPosition() {
        return inner.getPosition();
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, ValueAndTimestamp<V>> prefixScan(P prefix, PS prefixKeySerializer) {
        // TODO: conversion
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return inner.approximateNumEntries();
    }
}

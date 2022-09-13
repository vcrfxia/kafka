package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class CacheableChangeLoggingTimeAwareKeyValueBytesStore
    extends ChangeLoggingTimeAwareKeyValueBytesStore<CacheableVersionedKeyValueStore<Bytes, byte[]>>
    implements CacheableVersionedKeyValueStore<Bytes, byte[]> {

    CacheableChangeLoggingTimeAwareKeyValueBytesStore(final CacheableVersionedKeyValueStore<Bytes, byte[]> inner) {
        super(inner);
    }

    @Override
    public void replaceFromCache(Bytes key, ValueAndTimestamp<byte[]> value, long nextTimestamp) {
        wrapped().replaceFromCache(key, value, nextTimestamp);
        log(key, value);
    }

    @Override
    public void bypassCache(Bytes key, ValueAndTimestamp<byte[]> value, long nextTimestamp) {
        wrapped().bypassCache(key, value, nextTimestamp);
        log(key, value);
    }

    @Override
    public void newKeyInsertedToCache(Bytes key, long nextTimestamp) {
        wrapped().newKeyInsertedToCache(key, nextTimestamp);
        // TODO: interesting edge case here where we might update the underlying store and then
        // lose a cached record on restart. this risk is not new (already exists today).
    }
}

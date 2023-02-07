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

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedBytesStore;

/**
 * A {@link VersionedBytesStore} wrapper for writing changelog records on calls to
 * {@link VersionedBytesStore#put(Object, Object)} and {@link VersionedBytesStore#delete(Bytes, long)}.
 */
public class ChangeLoggingVersionedKeyValueBytesStore extends ChangeLoggingKeyValueBytesStore implements VersionedBytesStore {
    private static final Deserializer<ValueAndTimestamp<byte[]>> VALUE_AND_TIMESTAMP_DESERIALIZER
        = new NullableValueAndTimestampDeserializer<>(new ByteArrayDeserializer());

    private final VersionedBytesStore inner;

    ChangeLoggingVersionedKeyValueBytesStore(final KeyValueStore<Bytes, byte[]> inner) {
        super(inner);
        if (!(inner instanceof VersionedBytesStore)) {
            throw new IllegalStateException("inner store must be versioned");
        }
        this.inner = (VersionedBytesStore) inner;
    }

    @Override
    public byte[] get(Bytes key, long asOfTimestamp) {
        return inner.get(key, asOfTimestamp);
    }

    @Override
    public byte[] delete(Bytes key, long timestamp) {
        final byte[] oldValue = inner.delete(key, timestamp);
        log(key, ValueAndTimestamp.makeAllowNullable(null, timestamp));
        return oldValue;
    }

    @Override
    void log(final Bytes key, final byte[] rawValueAndTimestamp) {
        final ValueAndTimestamp<byte[]> valueAndTimestamp
            = VALUE_AND_TIMESTAMP_DESERIALIZER.deserialize(null, rawValueAndTimestamp);
        log(key, valueAndTimestamp);
    }

    private void log(final Bytes key, final ValueAndTimestamp<byte[]> valueAndTimestamp) {
        if (valueAndTimestamp == null) {
            throw new IllegalStateException("Serialized bytes to put for versioned store cannot be null");
        }
        context.logChange(
            name(),
            key,
            valueAndTimestamp.value(),
            valueAndTimestamp.timestamp(),
            wrapped().getPosition()
        );
    }
}
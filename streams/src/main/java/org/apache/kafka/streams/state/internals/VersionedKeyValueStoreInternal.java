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

import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.VersionedKeyValueStore;

/**
 * Internal representation of a {@link VersionedKeyValueStore} as a {@link TimestampedKeyValueStore}
 * so that all the internal code which expects key-value stores to be {@code TimestampedKeyValueStore}
 * instances can be re-used to support versioned key-value stores as well.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public interface VersionedKeyValueStoreInternal<K, V> extends TimestampedKeyValueStore<K, V> {

    /**
     * Equivalent to {@link VersionedKeyValueStore#get(Object, long)}.
     */
    ValueAndTimestamp<V> get(K key, long asOfTimestamp);

    /**
     * Equivalent to {@link VersionedKeyValueStore#delete(Object, long)}.
     */
    ValueAndTimestamp<V> delete(K key, long timestamp);
}
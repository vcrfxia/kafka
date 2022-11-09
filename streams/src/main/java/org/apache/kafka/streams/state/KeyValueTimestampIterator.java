package org.apache.kafka.streams.state;

import java.io.Closeable;
import java.util.Iterator;
import org.apache.kafka.streams.KeyValueTimestamp;

public interface KeyValueTimestampIterator<K, V> extends Iterator<KeyValueTimestamp<K, V>>, Closeable {

    @Override
    void close();

    K peekNextKey();
}

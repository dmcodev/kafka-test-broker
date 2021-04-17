package dev.dmcode.test.kafka.state.query.deserializer;

import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
public class MemoizedRecordDeserializer<V> implements RecordDeserializer<V> {

    private final RecordDeserializer<V> delegate;

    private final Map<Integer, V> memoized = new ConcurrentHashMap<>();

    @Override
    public V deserialize(byte[] bytes) {
        return memoized.computeIfAbsent(System.identityHashCode(bytes),
            identity -> delegate.deserialize(bytes));
    }
}

package dev.dmcode.test.kafka.state.query.deserializer;

import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RequiredArgsConstructor
@SuppressWarnings("unchecked")
public class MemoizedRecordDeserializer<K, V, HV> implements RecordDeserializer<K, V, HV> {

    private final RecordDeserializer<K, V, HV> delegate;

    private final Map<Integer, Object> memoized = new ConcurrentHashMap<>();

    @Override
    public K deserializeKey(byte[] key) {
        return (K) memoized.computeIfAbsent(System.identityHashCode(key),
            identity -> delegate.deserializeKey(key));
    }

    @Override
    public V deserializeValue(byte[] value) {
        return (V) memoized.computeIfAbsent(System.identityHashCode(value),
            identity -> delegate.deserializeValue(value));
    }

    @Override
    public HV deserializeHeaderValue(byte[] headerValue) {
        return (HV) memoized.computeIfAbsent(System.identityHashCode(headerValue),
            identity -> delegate.deserializeHeaderValue(headerValue));
    }
}

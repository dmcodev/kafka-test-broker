package dev.dmcode.test.kafka.state.query.view;

import dev.dmcode.test.kafka.messages.Record;
import dev.dmcode.test.kafka.state.query.deserializer.RecordDeserializer;
import lombok.Value;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Value
public class RecordView<K, V, HV> {

    int partitionId;
    long offset;
    K key;
    V value;
    Map<String, HV> headers;

    public static <K, V, HV> RecordView<K, V, HV> create(
        int partitionId,
        Record record,
        RecordDeserializer<K, V, HV> deserializer
    ) {
        K key = record.key().map(deserializer::deserializeKey).orElse(null);
        V value = record.value().map(deserializer::deserializeValue).orElse(null);
        Map<String, HV> headers = new HashMap<>();
        for (Record.Header header : record.headers()) {
            HV headerValue = header.value().map(deserializer::deserializeHeaderValue).orElse(null);
            headers.put(header.key(), headerValue);
        }
        return new RecordView<>(partitionId, record.offset(), key, value, headers);
    }

    public static <K, V, HV> RecordView<K, V, HV> map(
        RecordView<byte[], byte[], byte[]> record,
        RecordDeserializer<K, V, HV> deserializer
    ) {
        K key = Optional.ofNullable(record.getKey())
            .map(deserializer::deserializeKey).orElse(null);
        V value = Optional.ofNullable(record.getValue())
            .map(deserializer::deserializeValue).orElse(null);
        Map<String, HV> headers = new HashMap<>();
        for (Map.Entry<String, byte[]> header : record.getHeaders().entrySet()) {
            HV headerValue = Optional.ofNullable(header.getValue())
                .map(deserializer::deserializeHeaderValue).orElse(null);
            headers.put(header.getKey(), headerValue);
        }
        return new RecordView<>(record.getPartitionId(), record.getOffset(), key, value, headers);
    }
}

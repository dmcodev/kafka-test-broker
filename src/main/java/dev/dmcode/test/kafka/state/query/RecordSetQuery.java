package dev.dmcode.test.kafka.state.query;

import dev.dmcode.test.kafka.state.query.deserializer.MemoizedRecordDeserializer;
import dev.dmcode.test.kafka.state.query.deserializer.RecordDeserializer;
import dev.dmcode.test.kafka.state.query.view.RecordView;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class RecordSetQuery<K, V, HV> {

    private final Supplier<Stream<RecordView<byte[], byte[], byte[]>>> records;
    private final RecordDeserializer<K, V, HV> deserializer;
    private final QueryExecutor executor;

    public <NK, NV, NHV> RecordSetQuery<NK, NV, NHV> useDeserializer(RecordDeserializer<NK, NV, NHV> newDeserializer) {
        return new RecordSetQuery<>(records, memoized(newDeserializer), executor);
    }

    public RecordSetQuery<K, V, HV> filterByKey(Predicate<K> filter) {
        Supplier<Stream<RecordView<byte[], byte[], byte[]>>> filteredRecords = () -> records.get()
            .filter(record -> filter.test(deserializer.deserializeKey(record.getKey())));
        return new RecordSetQuery<>(filteredRecords, deserializer, executor);
    }

    public RecordSetQuery<K, V, HV> filterByValue(Predicate<V> filter) {
        Supplier<Stream<RecordView<byte[], byte[], byte[]>>> filteredRecords = () -> records.get()
            .filter(record -> filter.test(deserializer.deserializeValue(record.getValue())));
        return new RecordSetQuery<>(filteredRecords, deserializer, executor);
    }

    public List<RecordView<K, V, HV>> collect() {
        return records.get()
            .map(record -> mapRecordView(record, deserializer))
            .collect(Collectors.toList());
    }

    public RecordView<K, V, HV> collectSingle() {
        Collection<RecordView<K, V, HV>> results = collect();
        if (results.size() > 1) {
            throw new IllegalStateException("Multiple matching records found: " + results);
        }
        return results.stream().findFirst()
            .orElseThrow(() -> new IllegalStateException("No matching record found"));
    }

    private static  <K, V, HV> RecordDeserializer<K, V, HV> memoized(RecordDeserializer<K, V, HV> deserializer) {
        if (!(deserializer instanceof MemoizedRecordDeserializer)) {
            return new MemoizedRecordDeserializer<>(deserializer);
        }
        return deserializer;
    }

    private static <K, V, HV> RecordView<K, V, HV> mapRecordView(
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

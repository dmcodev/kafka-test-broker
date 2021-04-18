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
    private final RecordDeserializer<K> keyDeserializer;
    private final RecordDeserializer<V> valueDeserializer;
    private final RecordDeserializer<HV> headerValueDeserializer;
    private final QueryExecutor executor;

    public <NK> RecordSetQuery<NK, V, HV> keyDeserializer(RecordDeserializer<NK> deserializer) {
        return new RecordSetQuery<>(records, memoized(deserializer), valueDeserializer, headerValueDeserializer, executor);
    }

    public <NV> RecordSetQuery<K, NV, HV> valueDeserializer(RecordDeserializer<NV> deserializer) {
        return new RecordSetQuery<>(records, keyDeserializer, memoized(deserializer), headerValueDeserializer, executor);
    }

    public <KV> RecordSetQuery<KV, KV, HV> keyValueDeserializer(RecordDeserializer<KV> deserializer) {
        return new RecordSetQuery<>(records, memoized(deserializer), memoized(deserializer), headerValueDeserializer, executor);
    }

    public <NHV> RecordSetQuery<K, V, NHV> headerValueDeserializer(RecordDeserializer<NHV> deserializer) {
        return new RecordSetQuery<>(records, keyDeserializer, valueDeserializer, memoized(deserializer), executor);
    }

    public RecordSetQuery<K, V, HV> keyMatching(Predicate<K> filter) {
        Supplier<Stream<RecordView<byte[], byte[], byte[]>>> filteredRecords = () -> records.get()
            .filter(record -> filter.test(keyDeserializer.deserialize(record.key())));
        return withFilteredRecords(filteredRecords);
    }

    public RecordSetQuery<K, V, HV> valueMatching(Predicate<V> filter) {
        Supplier<Stream<RecordView<byte[], byte[], byte[]>>> filteredRecords = () -> records.get()
            .filter(record -> filter.test(valueDeserializer.deserialize(record.value())));
        return withFilteredRecords(filteredRecords);
    }

    public RecordSetQuery<K, V, HV> anyHeaderKeyMatching(Predicate<String> filter) {
        Supplier<Stream<RecordView<byte[], byte[], byte[]>>> filteredRecords = () -> records.get()
            .filter(record -> record.headers().keySet().stream().anyMatch(filter));
        return withFilteredRecords(filteredRecords);
    }

    public List<RecordView<K, V, HV>> all() {
        return records.get()
            .map(record -> mapRecordView(record, keyDeserializer, valueDeserializer, headerValueDeserializer))
            .collect(Collectors.toList());
    }

    public RecordView<K, V, HV> single() {
        Collection<RecordView<K, V, HV>> results = all();
        if (results.size() > 1) {
            throw new IllegalStateException("Multiple matching records found: " + results);
        }
        return results.stream().findFirst()
            .orElseThrow(() -> new IllegalStateException("No matching record found"));
    }

    private RecordSetQuery<K, V, HV> withFilteredRecords(Supplier<Stream<RecordView<byte[], byte[], byte[]>>> filteredRecords) {
        return new RecordSetQuery<>(filteredRecords, keyDeserializer, valueDeserializer, headerValueDeserializer, executor);
    }

    private static <V> RecordDeserializer<V> memoized(RecordDeserializer<V> deserializer) {
        if (!(deserializer instanceof MemoizedRecordDeserializer)) {
            return new MemoizedRecordDeserializer<>(deserializer);
        }
        return deserializer;
    }

    private static <K, V, HV> RecordView<K, V, HV> mapRecordView(
        RecordView<byte[], byte[], byte[]> record,
        RecordDeserializer<K> keyDeserializer,
        RecordDeserializer<V> valueDeserializer,
        RecordDeserializer<HV> headerValueDeserializer
    ) {
        K key = Optional.ofNullable(record.key())
            .map(keyDeserializer::deserialize).orElse(null);
        V value = Optional.ofNullable(record.value())
            .map(valueDeserializer::deserialize).orElse(null);
        Map<String, HV> headers = new HashMap<>();
        for (Map.Entry<String, byte[]> header : record.headers().entrySet()) {
            HV headerValue = Optional.ofNullable(header.getValue())
                .map(headerValueDeserializer::deserialize).orElse(null);
            headers.put(header.getKey(), headerValue);
        }
        return new RecordView<>(record.partitionId(), record.offset(), key, value, headers);
    }
}

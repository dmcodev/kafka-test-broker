package dev.dmco.test.kafka.state.view;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Value
@Builder
@Getter(AccessLevel.NONE)
public class RecordsView {

    Collection<RecordView> records;

    public Collection<RecordView> all() {
        return all(it -> true);
    }

    public Collection<RecordView> all(Predicate<RecordView> predicate) {
        return records.stream()
            .filter(predicate)
            .sorted(Comparator.comparingInt(RecordView::partitionIndex).thenComparingLong(RecordView::offset))
            .collect(Collectors.toList());
    }

    public RecordView first(Predicate<RecordView> predicate) {
        return records.stream()
            .filter(predicate)
            .findFirst()
            .orElse(null);
    }

    public RecordView firstByKey(byte[] key) {
        return first(it -> Arrays.equals(key, it.key()));
    }

    public Collection<RecordView> allByKey(byte[] key) {
        return all(it -> Arrays.equals(key, it.key()));
    }

    public RecordView firstByKey(String key) {
        return first(it -> Objects.equals(key, it.keyString()));
    }

    public Collection<RecordView> allByKey(String key) {
        return all(it -> Objects.equals(key, it.keyString()));
    }

    public RecordView firstByValue(byte[] value) {
        return first(it -> Arrays.equals(value, it.value()));
    }

    public Collection<RecordView> allByValue(byte[] value) {
        return all(it -> Arrays.equals(value, it.value()));
    }

    public RecordView firstByValue(String value) {
        return first(it -> Objects.equals(value, it.valueString()));
    }

    public Collection<RecordView> allByValue(String value) {
        return all(it -> Objects.equals(value, it.valueString()));
    }
}

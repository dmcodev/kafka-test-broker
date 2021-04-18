package dev.dmcode.test.kafka.state.query.view;

import lombok.Value;
import lombok.experimental.Accessors;

import java.util.Map;

@Value
@Accessors(fluent = true)
public class RecordView<K, V, HV> {
    int partitionId;
    long offset;
    K key;
    V value;
    Map<String, HV> headers;
}

package dev.dmcode.test.kafka.state.query.view;

import lombok.Value;

import java.util.Map;

@Value
public class RecordView<K, V, HV> {
    int partitionId;
    long offset;
    K key;
    V value;
    Map<String, HV> headers;
}

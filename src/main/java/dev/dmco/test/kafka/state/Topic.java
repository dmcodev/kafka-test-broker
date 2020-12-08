package dev.dmco.test.kafka.state;

import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
class Topic {

    private final Map<Integer, Partition> partitions = new HashMap<>();
    private final String name;

    Partition partition(int id) {
        return partitions.computeIfAbsent(id, Partition::new);
    }
}

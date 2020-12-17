package dev.dmco.test.kafka.state;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Accessors(fluent = true)
@RequiredArgsConstructor
public class Topic {

    private final Map<Integer, Partition> partitions = new HashMap<>();
    @Getter private final String name;

    public Partition partition(int partitionId) {
        return partitions.computeIfAbsent(partitionId, this::createPartition);
    }

    private Partition createPartition(int partitionId) {
        return new Partition(this, partitionId);
    }
}

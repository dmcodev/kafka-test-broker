package dev.dmco.test.kafka.state;

import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Accessors(fluent = true)
public class Topic {

    private final Map<Integer, Partition> partitions = new HashMap<>();
    @Getter private final String name;
    @Getter private final int partitionsNumber;

    public Topic(String name, int partitionsNumber) {
        this.name = name;
        this.partitionsNumber = partitionsNumber;
        initializePartitions();
    }

    public Partition getPartition(int partitionId) {
        return partitions.computeIfAbsent(partitionId, this::createPartition);
    }

    private Partition createPartition(int partitionId) {
        return new Partition(this, partitionId);
    }

    private void initializePartitions() {
        IntStream.range(0, partitionsNumber)
            .mapToObj(this::createPartition)
            .forEach(partition -> partitions.put(partition.id(), partition));
    }
}

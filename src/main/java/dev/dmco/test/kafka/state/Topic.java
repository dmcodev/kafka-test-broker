package dev.dmco.test.kafka.state;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Value
@Accessors(fluent = true)
@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Topic {

    Map<Integer, Partition> partitions = new HashMap<>();

    @ToString.Include
    @EqualsAndHashCode.Include
    String name;

    int partitionsNumber;

    public Topic(String name, int partitionsNumber) {
        this.name = name;
        this.partitionsNumber = partitionsNumber;
        initializePartitions();
    }

    public Partition partition(int partitionId) {
        return partitions.computeIfAbsent(partitionId, this::createPartition);
    }

    private Partition createPartition(int partitionId) {
        return new Partition(partitionId, this);
    }

    private void initializePartitions() {
        IntStream.range(0, partitionsNumber)
            .mapToObj(this::createPartition)
            .forEach(partition -> partitions.put(partition.id(), partition));
    }
}

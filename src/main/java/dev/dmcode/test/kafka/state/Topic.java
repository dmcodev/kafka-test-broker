package dev.dmcode.test.kafka.state;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Topic {

    private final Map<Integer, Partition> partitions = new HashMap<>();

    @Getter @EqualsAndHashCode.Include private final String name;
    @Getter private final int numberOfPartitions;

    public Topic(String name, int numberOfPartitions) {
        this.name = name;
        this.numberOfPartitions = numberOfPartitions;
        createInitialPartitions();
    }

    public Partition getOrCreatePartition(int partitionId) {
        return partitions.computeIfAbsent(partitionId, this::createPartition);
    }

    public Collection<Partition> partitions() {
        return partitions.values();
    }

    private Partition createPartition(int partitionId) {
        return new Partition(partitionId, this);
    }

    private void createInitialPartitions() {
        IntStream.range(0, numberOfPartitions)
            .mapToObj(this::createPartition)
            .forEach(partition -> partitions.put(partition.id(), partition));
    }
}

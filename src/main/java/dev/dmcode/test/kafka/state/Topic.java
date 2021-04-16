package dev.dmcode.test.kafka.state;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Topic {

    private final Map<Integer, Partition> partitions = new HashMap<>();

    @ToString.Include
    @EqualsAndHashCode.Include
    private final String name;

    private final int numberOfPartitions;

    public Topic(String name, int numberOfPartitions) {
        this.name = name;
        this.numberOfPartitions = numberOfPartitions;
        createPartitions();
    }

    public Collection<Partition> getPartitions() {
        return partitions.values();
    }

    public Partition getOrCreatePartition(int partitionId) {
        return partitions.computeIfAbsent(partitionId, this::createPartition);
    }

    public String getName() {
        return name;
    }

    public int getNumberOfPartitions() {
        return numberOfPartitions;
    }

    private Partition createPartition(int partitionId) {
        return new Partition(partitionId, this);
    }

    private void createPartitions() {
        IntStream.range(0, numberOfPartitions)
            .mapToObj(this::createPartition)
            .forEach(partition -> partitions.put(partition.getId(), partition));
    }
}

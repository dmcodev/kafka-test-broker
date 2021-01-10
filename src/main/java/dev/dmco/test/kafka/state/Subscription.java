package dev.dmco.test.kafka.state;

import lombok.RequiredArgsConstructor;

import java.util.HashSet;
import java.util.Set;

@RequiredArgsConstructor
public class Subscription {

    private final Set<Partition> partitions = new HashSet<>();

    public Set<Partition> getPartitions() {
        return new HashSet<>(partitions);
    }

    public void addPartition(Partition partition) {
        partitions.add(partition);
    }
}

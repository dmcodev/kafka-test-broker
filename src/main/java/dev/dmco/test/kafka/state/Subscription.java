package dev.dmco.test.kafka.state;

import lombok.RequiredArgsConstructor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@RequiredArgsConstructor
public class Subscription {

    private final Set<Partition> partitions = new HashSet<>();

    public Set<Partition> getPartitions() {
        return new HashSet<>(partitions);
    }

    public void setPartitions(Collection<Partition> partitions) {
        this.partitions.clear();
        this.partitions.addAll(partitions);
    }
}

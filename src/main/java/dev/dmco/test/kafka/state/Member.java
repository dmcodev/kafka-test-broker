package dev.dmco.test.kafka.state;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Accessors(fluent = true)
@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Member {

    static final String NAME_PREFIX = "member";

    private final Set<String> protocols = new HashSet<>();
    private final Map<String, Set<Partition>> assignedPartitions = new HashMap<>();

    @Getter
    @ToString.Include
    private final String id;

    @Getter
    private boolean isSynchronized;

    public void subscribe(Collection<String> topicNames) {
        topicNames.forEach(topic -> assignedPartitions.putIfAbsent(topic, new HashSet<>()));
    }

    public void assignPartitions(List<Partition> partitions) {
        assignedPartitions.values().forEach(Set::clear);
        partitions.forEach(partition -> assignedPartitions.get(partition.topic().name()).add(partition));
    }

    public List<String> subscribedTopics() {
        return new ArrayList<>(assignedPartitions.keySet());
    }

    public List<Partition> assignedPartitions() {
        return assignedPartitions.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    public Set<String> protocols() {
        return new HashSet<>(protocols);
    }

    public void setProtocols(Set<String> newProtocols) {
        protocols.clear();
        protocols.addAll(newProtocols);
    }

    public void invalidate() {
        isSynchronized = false;
    }

    public void synchronize() {
        isSynchronized = true;
    }
}

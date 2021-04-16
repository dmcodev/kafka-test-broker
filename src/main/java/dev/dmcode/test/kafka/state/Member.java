package dev.dmcode.test.kafka.state;

import dev.dmcode.test.kafka.logging.Logger;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Member {

    private static final Logger LOG = Logger.create(Member.class);

    static final String NAME_PREFIX = "member";

    private final Set<String> subscribedTopics = new HashSet<>();
    private final Set<Partition> assignedPartitions = new HashSet<>();
    private final Set<String> supportedProtocols = new HashSet<>();

    @EqualsAndHashCode.Include
    private final ConsumerGroup group;

    @Getter
    @EqualsAndHashCode.Include
    private final String id;

    @Getter
    private boolean partitionsSynced;

    @Getter
    private boolean isLeader;

    @Getter
    private int joinGenerationId;

    Member(ConsumerGroup consumerGroup, int sequenceNumber) {
        group = consumerGroup;
        id = NAME_PREFIX + "-" + sequenceNumber;
        partitionsSynced = true;
    }

    public void setAsLeader() {
        isLeader = true;
    }

    public Set<String> supportedProtocols() {
        return new HashSet<>(supportedProtocols);
    }

    public void assignProtocols(Set<String> protocols) {
        supportedProtocols.clear();
        supportedProtocols.addAll(protocols);
    }

    public Collection<String> subscribedTopics() {
        return new HashSet<>(subscribedTopics);
    }

    public void subscribe(Collection<String> topicNames) {
        subscribedTopics.clear();
        subscribedTopics.addAll(topicNames);
    }

    public void revokePartitions() {
        assignedPartitions.clear();
        partitionsSynced = false;
    }

    public void assignPartitions(List<Partition> partitions) {
        assignedPartitions.addAll(partitions);
        LOG.debug("{} assigned partitions: {}", this, partitions);
    }

    public Collection<Partition> synchronize() {
        partitionsSynced = true;
        LOG.debug("{} synchronized", this);
        return new HashSet<>(assignedPartitions);
    }

    public void setJoinGenerationId(int generationId) {
        joinGenerationId = generationId;
    }

    @Override
    public String toString() {
        return group.name() + ":" + id;
    }
}

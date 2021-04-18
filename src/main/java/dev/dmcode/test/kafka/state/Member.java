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

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Member {

    private static final Logger LOG = Logger.create(Member.class);

    static final String NAME_PREFIX = "member";

    private final Set<String> subscribedTopics = new HashSet<>();
    private final Set<String> supportedProtocols = new HashSet<>();
    private final Set<Partition> assignedPartitions = new HashSet<>();

    @EqualsAndHashCode.Include private final ConsumerGroup group;
    @EqualsAndHashCode.Include private final String id;

    private boolean partitionsSynchronized;
    private boolean isLeader;
    private int joinGenerationId;

    Member(ConsumerGroup consumerGroup, int sequenceNumber) {
        group = consumerGroup;
        id = NAME_PREFIX + "-" + sequenceNumber;
        partitionsSynchronized = true;
    }

    public void revokePartitions() {
        assignedPartitions.clear();
        partitionsSynchronized = false;
    }

    public Collection<Partition> synchronize() {
        partitionsSynchronized = true;
        LOG.debug("{} synchronized", this);
        return assignedPartitions;
    }

    void setAsLeader() {
        isLeader = true;
    }

    void assignPartitions(List<Partition> partitions) {
        assignedPartitions.addAll(partitions);
        LOG.debug("{} assigned partitions: {}", this, partitions);
    }

    void assignProtocols(Set<String> protocols) {
        supportedProtocols.clear();
        supportedProtocols.addAll(protocols);
    }

    void subscribe(Collection<String> topicNames) {
        subscribedTopics.clear();
        subscribedTopics.addAll(topicNames);
    }

    void setJoinGenerationId(int generationId) {
        joinGenerationId = generationId;
    }

    @Override
    public String toString() {
        return group.name() + ":" + id;
    }
}

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

    @Getter private final Set<String> subscribedTopics = new HashSet<>();
    @Getter private final Set<String> supportedProtocols = new HashSet<>();
    private final Set<Partition> assignedPartitions = new HashSet<>();

    @EqualsAndHashCode.Include private final ConsumerGroup group;
    @Getter @EqualsAndHashCode.Include private final String id;

    @Getter private boolean partitionsSynchronized;
    @Getter private boolean isLeader;
    @Getter private int joinGenerationId;

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
}

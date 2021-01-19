package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.messages.ErrorCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
@Accessors(fluent = true)
@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Member {

    static final String NAME_PREFIX = "member";

    private final Set<String> subscribedTopics = new HashSet<>();
    private final Set<Partition> assignedPartitions = new HashSet<>();
    private final ProtocolSet protocolSet = new ProtocolSet();

    @Getter
    @ToString.Include
    @EqualsAndHashCode.Include
    private final String id;

    private boolean isSynchronized;

    public Member(int sequenceNumber) {
        id = NAME_PREFIX + "-" + sequenceNumber;
    }

    public void subscribe(Collection<String> topicNames) {
        subscribedTopics.clear();
        subscribedTopics.addAll(topicNames);
    }

    public void assignPartitions(List<Partition> partitions) {
        assignedPartitions.clear();
        assignedPartitions.addAll(partitions);
    }

    public Collection<String> subscribedTopicNames() {
        return new HashSet<>(subscribedTopics);
    }

    public Collection<String> protocolNames() {
        return protocolSet.protocolNames();
    }

    public Member setProtocolNames(Set<String> protocolNames) {
        protocolSet.setProtocolNames(protocolNames);
        return this;
    }

    public void desynchronize() {
        isSynchronized = false;
        assignedPartitions.clear();
    }

    public Collection<Partition> synchronize() {
        isSynchronized = true;
        return new HashSet<>(assignedPartitions);
    }

    public ErrorCode validate() {
        if (!isSynchronized) {
            return ErrorCode.REBALANCE_IN_PROGRESS;
        }
        return ErrorCode.NO_ERROR;
    }
}

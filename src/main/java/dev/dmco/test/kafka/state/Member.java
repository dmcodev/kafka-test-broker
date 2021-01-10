package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.messages.ErrorCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.List;
import java.util.Set;

@RequiredArgsConstructor
@Accessors(fluent = true)
@ToString(onlyExplicitlyIncluded = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Member {

    static final String NAME_PREFIX = "member";

    private final ProtocolSet protocolSet = new ProtocolSet();
    private final Subscriptions subscriptions = new Subscriptions();

    @Getter
    @ToString.Include
    @EqualsAndHashCode.Include
    private final String id;

    private boolean isSynchronized;

    public Member(int sequenceNumber) {
        id = NAME_PREFIX + "-" + sequenceNumber;
    }

    public void subscribe(Collection<String> topicNames) {
        topicNames.forEach(subscriptions::getOrCreate);
    }

    public void assignPartitions(List<Partition> partitions) {
        partitions.forEach(partition -> subscriptions.getOrCreate(partition.topic().name()).addPartition(partition));
    }

    public Collection<String> subscribedTopicNames() {
        return subscriptions.topicNames();
    }

    public Collection<Partition> assignedPartitions() {
        return subscriptions.assignedPartitions();
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
    }

    public Member synchronize() {
        isSynchronized = true;
        return this;
    }

    public ErrorCode validate() {
        if (!isSynchronized) {
            return ErrorCode.REBALANCE_IN_PROGRESS;
        }
        return ErrorCode.NO_ERROR;
    }
}

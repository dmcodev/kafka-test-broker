package dev.dmcode.test.kafka.state;

import dev.dmcode.test.kafka.logging.Logger;
import dev.dmcode.test.kafka.messages.ErrorCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ConsumerGroup {

    private static final Logger LOG = Logger.create(ConsumerGroup.class);

    private final Map<String, Member> members = new HashMap<>();
    private final Map<Partition, Long> offsets = new HashMap<>();

    @Getter @EqualsAndHashCode.Include private final String name;

    @Getter private int generationId;
    @Getter private String leaderId;
    @Getter private String protocol;
    private int memberSequenceNumber;

    public String findMatchingProtocol(Set<String> protocols) {
        Collection<Set<String>> memberProtocolSets = members.values().stream()
            .map(Member::supportedProtocols)
            .collect(Collectors.toList());
        return protocols.stream()
            .filter(protocol -> memberProtocolSets.stream().allMatch(set -> set.contains(protocol)))
            .findFirst()
            .orElse(null);
    }

    public void setProtocol(String selectedProtocol) {
        protocol = selectedProtocol;
    }

    public boolean containsMember(String memberId) {
        return members.containsKey(memberId);
    }

    public Member getMember(String memberId) {
        return members.get(memberId);
    }

    public Member joinMember(String requestMemberId, Set<String> memberProtocols, Collection<String> subscriptionTopics) {
        Member member;
        if (!members.containsKey(requestMemberId)) {
            member = new Member(this, memberSequenceNumber++);
            members.put(member.id(), member);
            generationId++;
        } else {
            member = getMember(requestMemberId);
        }
        selectLeader(member);
        member.assignProtocols(memberProtocols);
        member.subscribe(subscriptionTopics);
        member.setJoinGenerationId(generationId);
        return member;
    }

    public void removeMember(String memberId) {
        Member member = members.remove(memberId);
        if (member != null) {
            if (member.isLeader()) {
                leaderId = null;
            }
            generationId++;
        }
    }

    public ErrorCode validateMember(String memberId) {
        if (!containsMember(memberId)) {
            return ErrorCode.UNKNOWN_MEMBER_ID;
        }
        Member member = members.get(memberId);
        if (shouldRejoin(member)) {
            return ErrorCode.REBALANCE_IN_PROGRESS;
        }
        return ErrorCode.NO_ERROR;
    }

    public void assignPartitions(Map<String, List<Partition>> assignments) {
        members.values().forEach(Member::revokePartitions);
        assignments.forEach((memberId, partitions) -> members.get(memberId).assignPartitions(partitions));
    }

    public Collection<Member> members() {
        return members.values();
    }

    public Map<Integer, Long> getPartitionOffsets(String topicName) {
        return offsets.keySet().stream()
            .filter(partition -> topicName.equals(partition.topic().name()))
            .collect(Collectors.toMap(Partition::id, this::getCommittedOffset));
    }

    public void commit(Partition partition, long offset) {
        offsets.put(partition, offset);
    }

    private long getCommittedOffset(Partition partition) {
        return offsets.computeIfAbsent(partition, it -> 0L);
    }

    private void selectLeader(Member member) {
        if (Objects.isNull(leaderId)) {
            leaderId = member.id();
            member.setAsLeader();
            LOG.debug("{} selected as a leader", member);
        }
    }

    private boolean shouldRejoin(Member member) {
        return Objects.isNull(leaderId)
            || (member.isLeader() && member.joinGenerationId() != generationId)
            || !member.partitionsSynchronized();
    }

    @Override
    public String toString() {
        return name;
    }
}

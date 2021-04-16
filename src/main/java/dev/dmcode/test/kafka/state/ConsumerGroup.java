package dev.dmcode.test.kafka.state;

import dev.dmcode.test.kafka.logging.Logger;
import dev.dmcode.test.kafka.messages.ErrorCode;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class ConsumerGroup {

    private static final Logger LOG = Logger.create(ConsumerGroup.class);

    private final Map<String, Member> members = new HashMap<>();
    private final Map<Partition, Long> offsets = new HashMap<>();

    @EqualsAndHashCode.Include
    private final String name;

    private int generationId;
    private String leaderId;
    private String protocol;
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

    public Member addMember() {
        Member member = new Member(this, memberSequenceNumber++);
        members.put(member.id(), member);
        LOG.debug("{} joined consumer group", member);
        ensureLeaderSelected(member);
        generationId++;
        return member;
    }

    public Member rejoinMember(String memberId) {
        Member member = getMember(memberId);
        LOG.debug("{} rejoined consumer group", member);
        ensureLeaderSelected(member);
        return member;
    }

    public void removeMember(String memberId) {
        Member member = members.remove(memberId);
        if (member != null) {
            if (member.isLeader()) {
                leaderId = null;
                LOG.debug("{} leader left consumer group", member);
            } else {
                LOG.debug("{} member left consumer group", member);
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

    public boolean assignPartitions(Map<String, List<Partition>> assignments) {
        if (!containsAllMembers(assignments.keySet())) {
            LOG.debug("{} Cannot assign partitions, some members already left the group", this);
            return false;
        }
        members.values().forEach(Member::revokePartitions);
        assignments.forEach((memberId, partitions) -> members.get(memberId).assignPartitions(partitions));
        return true;
    }

    public List<Member> getMembers() {
        return new ArrayList<>(members.values());
    }

    public Map<Integer, Long> getPartitionOffsets(String topicName) {
        return offsets.keySet().stream()
            .filter(partition -> topicName.equals(partition.getTopic().getName()))
            .collect(Collectors.toMap(Partition::getId, this::committedOffset));
    }

    public void commit(Partition partition, long offset) {
        offsets.put(partition, offset);
    }

    private boolean containsAllMembers(Collection<String> memberIds) {
        return memberIds.stream().allMatch(members::containsKey);
    }

    private long committedOffset(Partition partition) {
        return offsets.computeIfAbsent(partition, it -> 0L);
    }

    private void ensureLeaderSelected(Member member) {
        if (Objects.isNull(leaderId)) {
            leaderId = member.id();
            member.setAsLeader();
            LOG.debug("{} selected as a leader", member);
        }
    }

    private boolean shouldRejoin(Member member) {
        return Objects.isNull(leaderId)
            || (member.isLeader() && member.joinGenerationId() != generationId)
            || !member.partitionsSynced();
    }

    @Override
    public String toString() {
        return name;
    }
}

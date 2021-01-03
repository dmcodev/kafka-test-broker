package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.messages.ErrorCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Accessors(fluent = true)
public class ConsumerGroup {

    private final Map<String, Member> members = new HashMap<>();
    private final Map<Partition, Long> offsets = new HashMap<>();

    @Getter
    private int generationId = -1;

    @Getter
    private String leaderId;

    @Getter
    private String protocol;

    private int nextMemberId;

    public String findMatchingProtocol(Set<String> candidateProtocols) {
        Set<String> candidates = new HashSet<>(candidateProtocols);
        members.values().stream().map(Member::protocols).forEach(candidates::retainAll);
        return candidates.stream().findFirst().orElse(null);
    }

    public boolean hasMember(String memberId) {
        return members.containsKey(memberId);
    }

    public Member joinMember() {
        Member member = new Member(generateMemberId());
        if (noLeaderSelected()) {
            leaderId = member.id();
        }
        members.put(member.id(), member);
        nextGeneration();
        return member;
    }

    public Member getMember(String memberId) {
        return members.get(memberId);
    }

    public void removeMember(String memberId) {
        if (members.remove(memberId) != null) {
            if (memberId.equals(leaderId)) {
                leaderId = null;
            }
            nextGeneration();
        }
    }

    public boolean isLeader(Member member) {
        return member.id().equals(leaderId);
    }

    public void assignPartitions(Map<String, List<Partition>> assignedPartitions) {
        assignedPartitions.forEach(this::assignMemberPartitions);
        invalidateMembers();
    }

    public List<Partition> getAssignedPartitions(String memberId) {
        return Optional.ofNullable(members.get(memberId))
            .map(Member::assignedPartitions)
            .orElseGet(Collections::emptyList);
    }

    public List<Member> getMembers() {
        return new ArrayList<>(members.values());
    }

    public ErrorCode checkMemberSynchronization(String memberId) {
        if (!members.containsKey(memberId)) {
            return ErrorCode.UNKNOWN_MEMBER_ID;
        }
        if (!members.get(memberId).isSynchronized()) {
            return ErrorCode.REBALANCE_IN_PROGRESS;
        }
        return ErrorCode.NO_ERROR;
    }

    public void markSynchronized(String memberId) {
        members.get(memberId).synchronize();
    }

    public Map<Integer, Long> getPartitionOffsets(String topicName) {
        return offsets.entrySet().stream()
            .filter(entry -> topicName.equals(entry.getKey().topic().name()))
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().id(),
                    entry -> offsets.computeIfAbsent(entry.getKey(), it -> 0L)
                )
            );
    }

    public void commit(Partition partition, long offset) {
        offsets.put(partition, offset);
    }

    public void setProtocol(String selectedProtocol) {
        protocol = selectedProtocol;
    }

    private void assignMemberPartitions(String memberId, List<Partition> partitions) {
        Optional.ofNullable(members.get(memberId))
            .ifPresent(member -> member.assignPartitions(partitions));
    }

    private void nextGeneration() {
        generationId++;
        invalidateMembers();
    }

    private void invalidateMembers() {
        members.values().forEach(Member::invalidate);
    }

    private boolean noLeaderSelected() {
        return leaderId == null;
    }

    private String generateMemberId() {
        return Member.NAME_PREFIX + "-" + (nextMemberId++);
    }
}

package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.messages.ErrorCode;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
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
    private int generationId;

    @Getter
    private String leaderId;

    @Getter
    private String protocol;

    private int nextMemberId;

    public AddMemberResult addMember(String memberId, Set<String> proposedProtocols) {
        Member member;
        if (members.containsKey(memberId)) {
            member = members.get(memberId);
        } else {
            member = new Member(generateMemberId());
        }
        String selectedProtocol = selectProtocol(proposedProtocols);
        if (selectedProtocol == null) {
            removeMember(member.id());
            return AddMemberResult.builder().protocolMatched(false).build();
        }
        member.setProtocols(proposedProtocols);
        members.put(member.id(), member);
        protocol = selectedProtocol;
        if (noLeaderSelected()) {
            leaderId = member.id();
        } else {
            setupNextGeneration();
        }
        return AddMemberResult.builder()
            .protocolMatched(true)
            .member(member)
            .build();
    }

    public void removeMember(String memberId) {
        if (members.remove(memberId) != null) {
            setupNextGeneration();
        }
    }

    public void assignPartitions(Map<String, List<Partition>> assignedPartitions) {
        assignedPartitions.forEach(this::assignMemberPartitions);
        invalidateMembers();
    }

    public void markSynchronized(String memberId) {
        Optional.ofNullable(members.get(memberId))
            .ifPresent(Member::markSynchronized);
    }

    public List<Partition> getAssignedPartitions(String memberId) {
        return Optional.ofNullable(members.get(memberId))
            .map(Member::assignedPartitions)
            .orElseGet(Collections::emptyList);
    }

    public List<Member> getMembers() {
        return new ArrayList<>(members.values());
    }

    public ErrorCode validateMember(String memberId, int expectedGenerationId) {
        if (!members.containsKey(memberId)) {
            return ErrorCode.UNKNOWN_MEMBER_ID;
        }
        if (generationId != expectedGenerationId || !members.get(memberId).inSync()) {
            return ErrorCode.REBALANCE_IN_PROGRESS;
        }
        return ErrorCode.NO_ERROR;
    }

    public Map<Integer, Long> getPartitionOffsets(String topicName) {
        return offsets.keySet().stream()
            .filter(partition -> topicName.equals(partition.topic().name()))
            .collect(
                Collectors.toMap(
                    Partition::id,
                    partition -> offsets.computeIfAbsent(partition, it -> 0L)
                )
            );
    }

    private String selectProtocol(Set<String> candidateProtocols) {
        Set<String> candidates = new HashSet<>(candidateProtocols);
        members.values().stream().map(Member::protocols).forEach(candidates::retainAll);
        return candidates.stream().findFirst().orElse(null);
    }

    private void assignMemberPartitions(String memberId, List<Partition> partitions) {
        Optional.ofNullable(members.get(memberId))
            .ifPresent(member -> member.assignPartitions(partitions));
    }

    private void setupNextGeneration() {
        generationId++;
        invalidateMembers();
    }

    private void invalidateMembers() {
        members.values().forEach(Member::markDesynchronized);
    }

    private boolean noLeaderSelected() {
        return leaderId == null;
    }

    private String generateMemberId() {
        return Member.NAME_PREFIX + "-" + (nextMemberId++);
    }

    @Value
    @Builder
    @Accessors(fluent = true)
    public static class AddMemberResult {
        boolean protocolMatched;
        Member member;
    }
}

package dev.dmco.test.kafka.state;

import dev.dmco.test.kafka.logging.Logger;
import dev.dmco.test.kafka.messages.ErrorCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Accessors(fluent = true)
public class ConsumerGroup {

    private static final Logger LOG = Logger.create(ConsumerGroup.class);

    private final Map<String, Member> members = new HashMap<>();
    private final Map<Partition, Long> offsets = new HashMap<>();

    @Getter
    private int generationId = -1;

    @Getter
    private String leaderId;

    @Getter
    private String protocol;

    private int nextMemberId;

    public String findMatchingProtocolName(Set<String> protocolNames) {
        return members.values().stream()
            .map(Member::protocolNames)
            .map(ProtocolSet::new)
            .reduce(ProtocolSet::merge)
            .orElseGet(() -> new ProtocolSet(protocolNames))
            .findMatchingProtocolName(new ProtocolSet(protocolNames));
    }

    public boolean containsMember(String memberId) {
        return members.containsKey(memberId);
    }

    public Member joinMember() {
        Member member = new Member(nextMemberId++);
        if (noLeaderSelected()) {
            leaderId = member.id();
        }
        members.put(member.id(), member);
        advanceGeneration();
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
            advanceGeneration();
        }
    }

    public ErrorCode validateMember(String memberId) {
        if (!members.containsKey(memberId)) {
            return ErrorCode.UNKNOWN_MEMBER_ID;
        }
        return members.get(memberId).validate();
    }

    public boolean isLeader(Member member) {
        return member.id().equals(leaderId);
    }

    public void assignPartitions(Map<String, List<Partition>> assignedPartitions) {
        LOG.info("Partitions assignment: {}", assignedPartitions);
        desynchronizeMembers();
        assignedPartitions.forEach(this::assignPartitions);
    }

    public List<Member> getMembers() {
        return new ArrayList<>(members.values());
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

    private void assignPartitions(String memberId, List<Partition> partitions) {
        Optional.ofNullable(members.get(memberId))
            .ifPresent(member -> member.assignPartitions(partitions));
    }

    private void advanceGeneration() {
        generationId++;
        desynchronizeMembers();
    }

    private void desynchronizeMembers() {
        members.values().forEach(Member::desynchronize);
    }

    private boolean noLeaderSelected() {
        return leaderId == null;
    }
}

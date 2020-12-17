package dev.dmco.test.kafka.state;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class ConsumerGroup {

    private final Map<String, Member> members = new HashMap<>();
    private final String name;

    private int generationId;
    private String protocol;
    private String leaderId;
    private int nextMemberId;

    public Optional<String> selectProtocol(Set<String> candidateProtocols) {
        Set<String> candidates = new HashSet<>(candidateProtocols);
        members.values().stream().map(Member::getProtocols).forEach(candidates::retainAll);
        return candidates.stream().findFirst();
    }

    public AddMemberResult addMember(
        String lastMemberId,
        Set<String> memberProtocols,
        String selectedProtocol,
        Map<String, List<Integer>> assignments
    ) {
        String memberId = lastMemberId.isEmpty() ? generateMemberId() : lastMemberId;
        Member member = new Member(memberId, memberProtocols);
        member.setAssignments(assignments);
        members.put(memberId, member);
        if (noLeaderSelected()) {
            leaderId = memberId;
        }
        protocol = selectedProtocol;
        return AddMemberResult.builder()
            .leaderId(leaderId)
            .memberId(memberId)
            .generationId(generationId++)
            .build();
    }

    public List<MemberSnapshot> membersSnapshot() {
        return members.values().stream()
            .map(Member::getSnapshot)
            .collect(Collectors.toList());
    }

    private String generateMemberId() {
        return Member.NAME_PREFIX + "-" + (nextMemberId++);
    }

    private boolean noLeaderSelected() {
        return leaderId == null;
    }

    @Value
    @Builder
    @Accessors(fluent = true)
    public static class AddMemberResult {
        String leaderId;
        String memberId;
        int generationId;
    }
}

package dev.dmco.test.kafka.state;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RequiredArgsConstructor
public class ConsumerGroup {

    private final Map<String, Member> members = new HashMap<>();
    private final String name;

    private int generationId;
    private String protocol;
    private String leaderId;
    private int nextMemberId;

    public Optional<String> selectProtocol(Member candidateMember) {
        Set<String> candidates = new HashSet<>(candidateMember.protocols());
        members.values().stream().map(Member::protocols).forEach(candidates::retainAll);
        return candidates.stream().findFirst();
    }

    public AddMemberResult addMember(Member candidateMember, String selectedProtocol) {
        Member member = assignId(candidateMember);
        members.put(member.id(), member);
        protocol = selectedProtocol;
        if (noLeaderSelected()) {
            leaderId = member.id();
        } else {
            generationId++;
        }
        return AddMemberResult.builder()
            .leaderId(leaderId)
            .memberId(member.id())
            .generationId(generationId)
            .build();
    }

    public List<Member> members() {
        return new ArrayList<>(members.values());
    }

    private Member assignId(Member member) {
        if (member.id().isEmpty()) {
            return member.withId(Member.NAME_PREFIX + "-" + (nextMemberId++));
        } else {
            return member;
        }
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

        public boolean isLeaderAssignment() {
            return leaderId.equals(memberId);
        }
    }
}

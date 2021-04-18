package dev.dmcode.test.kafka.usecase.joingroup;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.messages.consumer.Subscription;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.ConsumerGroup;
import dev.dmcode.test.kafka.state.Member;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinGroupRequestHandler implements RequestHandler<JoinGroupRequest, JoinGroupResponse> {

    private static final JoinGroupResponse PROTOCOL_MISMATCH_RESPONSE = JoinGroupResponse.builder()
        .errorCode(ErrorCode.INCONSISTENT_GROUP_PROTOCOL)
        .build();

    @Override
    public void handle(JoinGroupRequest request, BrokerState state, ResponseScheduler<JoinGroupResponse> scheduler) {
        ConsumerGroup group = state.getOrCreateConsumerGroup(request.groupId());
        Set<String> memberProtocols = extractMembersProtocols(request);
        String selectedProtocol = group.findMatchingProtocol(memberProtocols);
        if (selectedProtocol == null) {
            scheduler.scheduleResponse(PROTOCOL_MISMATCH_RESPONSE);
            return;
        }
        group.setProtocol(selectedProtocol);
        Subscription subscription = extractSubscription(request, selectedProtocol);
        Member member = group.joinMember(request.memberId(), memberProtocols, subscription.topics());
        JoinGroupResponse response = buildResponse(member, group, subscription);
        scheduler.scheduleResponse(response);
    }

    private Set<String> extractMembersProtocols(JoinGroupRequest request) {
        return request.protocols().stream()
            .map(JoinGroupRequest.Protocol::name)
            .collect(Collectors.toSet());
    }

    private Subscription extractSubscription(JoinGroupRequest request, String selectedProtocol) {
        return request.protocols().stream()
            .filter(protocol -> protocol.name().equals(selectedProtocol))
            .findFirst()
            .map(JoinGroupRequest.Protocol::subscription)
            .orElseThrow(() -> new IllegalArgumentException("Could not find matching subscription"));
    }

    private JoinGroupResponse buildResponse(Member member, ConsumerGroup group, Subscription subscription) {
        return JoinGroupResponse.builder()
            .generationId(group.generationId())
            .protocolName(group.protocol())
            .leader(group.leaderId())
            .memberId(member.id())
            .members(member.isLeader() ? buildMembersInfo(subscription, group) : Collections.emptyList())
            .build();
    }

    private List<JoinGroupResponse.Member> buildMembersInfo(Subscription subscription, ConsumerGroup group) {
        short protocolVersion = subscription.version();
        return group.getMembers().stream()
            .map(member -> buildMemberInfo(member, protocolVersion))
            .collect(Collectors.toList());
    }

    private JoinGroupResponse.Member buildMemberInfo(Member member, short protocolVersion) {
        return JoinGroupResponse.Member.builder()
            .memberId(member.id())
            .subscription(
                Subscription.builder()
                    .version(protocolVersion)
                    .topics(member.subscribedTopics())
                    .build()
            )
            .build();
    }
}

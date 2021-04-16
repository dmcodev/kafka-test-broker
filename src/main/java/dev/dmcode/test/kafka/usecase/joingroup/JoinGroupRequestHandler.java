package dev.dmcode.test.kafka.usecase.joingroup;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.messages.consumer.Subscription;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.ConsumerGroup;
import dev.dmcode.test.kafka.state.Member;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;
import dev.dmcode.test.kafka.usecase.joingroup.JoinGroupResponse.JoinGroupResponseBuilder;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinGroupRequestHandler implements RequestHandler<JoinGroupRequest, JoinGroupResponse> {

    private static final JoinGroupResponse PROTOCOL_MISMATCH_RESPONSE = JoinGroupResponse.builder()
        .errorCode(ErrorCode.INCONSISTENT_GROUP_PROTOCOL)
        .build();

    @Override
    public void handle(JoinGroupRequest request, BrokerState state, ResponseScheduler<JoinGroupResponse> scheduler) {
        String groupId = request.groupId();
        ConsumerGroup group = state.getOrCreateConsumerGroup(groupId);
        Set<String> memberProtocols = extractMembersProtocols(request);
        String selectedProtocol = group.findMatchingProtocol(memberProtocols);
        if (selectedProtocol == null) {
            scheduler.scheduleResponse(PROTOCOL_MISMATCH_RESPONSE);
            return;
        }
        Member member;
        if (group.containsMember(request.memberId())) {
            member = group.rejoinMember(request.memberId());
        } else {
            member = group.addMember();
        }
        group.setProtocol(selectedProtocol);
        member.assignProtocols(memberProtocols);
        member.setJoinGenerationId(group.generationId());
        Subscription subscription = extractSubscription(request, selectedProtocol);
        member.subscribe(subscription.topics());
        JoinGroupResponse response = buildGroupInfo(member, group, subscription);
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

    private JoinGroupResponse buildGroupInfo(Member member, ConsumerGroup group, Subscription subscription) {
        JoinGroupResponseBuilder responseBuilder = JoinGroupResponse.builder()
            .generationId(group.generationId())
            .protocolName(group.protocol())
            .leader(group.leaderId())
            .memberId(member.id());
        if (member.isLeader()) {
            responseBuilder.members(buildMembersInfo(subscription, group));
        }
        return responseBuilder.build();
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

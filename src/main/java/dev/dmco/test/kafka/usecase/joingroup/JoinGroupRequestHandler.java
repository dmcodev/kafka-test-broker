package dev.dmco.test.kafka.usecase.joingroup;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.messages.consumer.Subscription;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.Member;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.joingroup.JoinGroupResponse.JoinGroupResponseBuilder;

import java.util.Set;
import java.util.stream.Collectors;

public class JoinGroupRequestHandler implements RequestHandler<JoinGroupRequest, JoinGroupResponse> {

    private static final JoinGroupResponse PROTOCOL_MISMATCH_RESPONSE = JoinGroupResponse.builder()
        .errorCode(ErrorCode.INCONSISTENT_GROUP_PROTOCOL)
        .build();

    @Override
    public JoinGroupResponse handle(JoinGroupRequest request, BrokerState state) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        Set<String> memberProtocols = extractProtocolNames(request);
        String selectedProtocol = group.findMatchingProtocolName(memberProtocols);
        if (selectedProtocol == null) {
            return PROTOCOL_MISMATCH_RESPONSE;
        }
        Member member = group.containsMember(request.memberId())
            ? group.getMember(request.memberId())
            : group.joinMember();
        group.setProtocol(selectedProtocol);
        Subscription subscription = extractSubscription(request, selectedProtocol);
        member.setProtocolNames(memberProtocols)
            .subscribe(subscription.topics());
        JoinGroupResponseBuilder responseBuilder = createResponseBuilder(member.id(), group);
        if (group.isLeader(member)) {
            addMemberInfo(subscription, group, responseBuilder);
        }
        return responseBuilder.build();
    }

    private Set<String> extractProtocolNames(JoinGroupRequest request) {
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

    private void addMemberInfo(Subscription subscription, ConsumerGroup group, JoinGroupResponseBuilder responseBuilder) {
        short protocolVersion = subscription.version();
        group.getMembers().stream()
            .map(member -> createResponseMember(member, protocolVersion))
            .forEach(responseBuilder::member);
    }

    private JoinGroupResponse.Member createResponseMember(Member member, short protocolVersion) {
        return JoinGroupResponse.Member.builder()
            .memberId(member.id())
            .subscription(
                Subscription.builder()
                    .version(protocolVersion)
                    .topics(member.subscribedTopicNames())
                    .build()
            )
            .build();
    }

    private JoinGroupResponseBuilder createResponseBuilder(String memberId, ConsumerGroup group) {
        return JoinGroupResponse.builder()
            .generationId(group.generationId())
            .protocolName(group.protocol())
            .leader(group.leaderId())
            .memberId(memberId);
    }
}

package dev.dmco.test.kafka.usecase.joingroup;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.messages.consumer.Subscription;
import dev.dmco.test.kafka.messages.consumer.Subscription.PartitionAssignments;
import dev.dmco.test.kafka.state.AssignedPartitions;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.ConsumerGroup.AddMemberResult;
import dev.dmco.test.kafka.state.Member;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.joingroup.JoinGroupResponse.JoinGroupResponseBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JoinGroupRequestHandler implements RequestHandler<JoinGroupRequest, JoinGroupResponse> {

    private static final JoinGroupResponse PROTOCOL_MISMATCH_RESPONSE = JoinGroupResponse.builder()
        .errorCode(ErrorCode.INCONSISTENT_GROUP_PROTOCOL)
        .build();

    @Override
    public JoinGroupResponse handle(JoinGroupRequest request, BrokerState state) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        Member candidateMember = createCandidateMember(request);
        Optional<String> candidateProtocol = group.selectProtocol(candidateMember);
        if (!candidateProtocol.isPresent()) {
            return PROTOCOL_MISMATCH_RESPONSE;
        }
        String selectedProtocol = candidateProtocol.get();
        Subscription subscription = extractSubscription(request, selectedProtocol);
        Member candidateMemberWithAssignments =
            candidateMember.withPartitionAssignments(extractPartitionAssignments(subscription));
        AddMemberResult result = group.addMember(candidateMemberWithAssignments, selectedProtocol);
        JoinGroupResponseBuilder responseBuilder = createResponseBuilder(result, selectedProtocol);
        if (result.isLeaderAssignment()) {
            addMemberInfo(subscription, group, responseBuilder);
        }
        return responseBuilder.build();
    }

    private Member createCandidateMember(JoinGroupRequest request) {
        return Member.builder()
            .id(request.memberId())
            .protocols(extractProtocolNames(request))
            .build();
    }

    private Set<String> extractProtocolNames(JoinGroupRequest request) {
        return request.protocols().stream()
            .map(JoinGroupRequest.Protocol::name)
            .collect(Collectors.toSet());
    }

    private List<AssignedPartitions> extractPartitionAssignments(Subscription subscription) {
        Map<String, List<Integer>> existingAssignments = subscription.partitionAssignments().stream()
            .collect(Collectors.toMap(PartitionAssignments::topicName, PartitionAssignments::partitionIds));
        Map<String, List<Integer>> emptyAssignments = subscription.topics().stream()
            .filter(it -> !existingAssignments.containsKey(it))
            .collect(Collectors.toMap(Function.identity(), it -> Collections.emptyList()));
        return Stream.of(existingAssignments, emptyAssignments)
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .map(it -> AssignedPartitions.builder()
                .topicName(it.getKey())
                .partitionIds(it.getValue())
                .build()
            )
            .collect(Collectors.toList());
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
        group.members().stream()
            .map(member -> createResponseMember(member, protocolVersion))
            .forEach(responseBuilder::member);
    }

    private JoinGroupResponse.Member createResponseMember(Member member, short protocolVersion) {
        return JoinGroupResponse.Member.builder()
            .memberId(member.id())
            .subscription(
                Subscription.builder()
                    .version(protocolVersion)
                    .topics(member.subscribedTopics())
                    .partitionAssignments(createResponsePartitionAssignments(member))
                    .build()
            )
            .build();
    }

    private List<PartitionAssignments> createResponsePartitionAssignments(Member member) {
        return member.partitionAssignments().stream()
            .map(assignments ->
                PartitionAssignments.builder()
                    .topicName(assignments.topicName())
                    .partitionIds(assignments.partitionIds())
                    .build()
            )
            .collect(Collectors.toList());
    }

    private JoinGroupResponseBuilder createResponseBuilder(AddMemberResult result, String selectedProtocol) {
        return JoinGroupResponse.builder()
            .generationId(result.generationId())
            .protocolName(selectedProtocol)
            .leader(result.leaderId())
            .memberId(result.memberId());
    }
}

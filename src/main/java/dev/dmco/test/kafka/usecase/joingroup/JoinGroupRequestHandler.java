package dev.dmco.test.kafka.usecase.joingroup;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.messages.consumer.Subscription;
import dev.dmco.test.kafka.messages.consumer.Subscription.PartitionAssignments;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.ConsumerGroup.AddMemberResult;
import dev.dmco.test.kafka.state.MemberSnapshot;
import dev.dmco.test.kafka.usecase.RequestHandler;

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

    @Override
    public JoinGroupResponse handle(JoinGroupRequest request, BrokerState state) {
        ConsumerGroup group = state.consumerGroup(request.groupId());
        Set<String> protocols = request.protocols().stream()
            .map(JoinGroupRequest.Protocol::name)
            .collect(Collectors.toSet());
        Optional<String> potentialSelectedProtocol = group.selectProtocol(protocols);
        if (!potentialSelectedProtocol.isPresent()) {
            return JoinGroupResponse.builder()
                .errorCode(ErrorCode.INCONSISTENT_GROUP_PROTOCOL)
                .build();
        }
        String selectedProtocol = potentialSelectedProtocol.get();
        Subscription subscription = subscription(request, selectedProtocol);
        Map<String, List<Integer>> partitionAssignments = partitionAssignments(subscription);
        AddMemberResult result = group.addMember(request.memberId(), protocols, selectedProtocol, partitionAssignments);
        JoinGroupResponse.JoinGroupResponseBuilder responseBuilder = responseBuilder(result, selectedProtocol);
        if (result.leaderId().equals(result.memberId())) {
            short protocolVersion = subscription.version();
            group.membersSnapshot().stream()
                .map(snapshot -> responseMember(snapshot, protocolVersion))
                .forEach(responseBuilder::member);
        }
        return responseBuilder.build();
    }

    private Map<String, List<Integer>> partitionAssignments(Subscription subscription) {
        Map<String, List<Integer>> existingAssignments = subscription.partitionAssignments().stream()
            .collect(Collectors.toMap(PartitionAssignments::topicName, PartitionAssignments::partitions));
        Map<String, List<Integer>> emptyAssignments = subscription.topics().stream()
            .filter(it -> !existingAssignments.containsKey(it))
            .collect(Collectors.toMap(Function.identity(), it -> Collections.emptyList()));
        return Stream.of(existingAssignments, emptyAssignments)
            .map(Map::entrySet)
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private JoinGroupResponse.Member responseMember(MemberSnapshot snapshot, short protocolVersion) {
        return JoinGroupResponse.Member.builder()
            .memberId(snapshot.id())
            .subscription(
                Subscription.builder()
                    .version(protocolVersion)
                    .topics(snapshot.topics())
                    .partitionAssignments(
                        snapshot.partitions().entrySet().stream()
                            .map(entry ->
                                PartitionAssignments.builder()
                                    .topicName(entry.getKey())
                                    .partitions(entry.getValue())
                                    .build()
                            )
                            .collect(Collectors.toList())
                    )
                    .build()
            )
            .build();
    }

    private JoinGroupResponse.JoinGroupResponseBuilder responseBuilder(AddMemberResult result, String selectedProtocol) {
        return JoinGroupResponse.builder()
            .generationId(result.generationId())
            .protocolName(selectedProtocol)
            .leader(result.leaderId())
            .memberId(result.memberId());
    }

    private Subscription subscription(JoinGroupRequest request, String selectedProtocol) {
        return request.protocols().stream()
            .filter(protocol -> protocol.name().equals(selectedProtocol))
            .findFirst()
            .map(JoinGroupRequest.Protocol::subscription)
            .orElseThrow(() -> new IllegalArgumentException("Could not find matching subscription"));
    }
}

package dev.dmco.test.kafka.usecase.syncgroup;

import dev.dmco.test.kafka.messages.consumer.Assignment;
import dev.dmco.test.kafka.state.AssignedPartitions;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SyncGroupRequestHandler implements RequestHandler<SyncGroupRequest, SyncGroupResponse> {

    @Override
    public SyncGroupResponse handle(SyncGroupRequest request, BrokerState state) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        Map<String, List<AssignedPartitions>> assignedPartitions = extractAssignedPartitions(request);
        group.assignPartitions(assignedPartitions);
        List<AssignedPartitions> memberAssignedPartitions = group.getAssignment(request.memberId());
        return SyncGroupResponse.builder()
            .assignment(createResponseAssignment(memberAssignedPartitions))
            .build();
    }

    private Map<String, List<AssignedPartitions>> extractAssignedPartitions(SyncGroupRequest request) {
        return request.memberAssignments().stream()
            .collect(
                Collectors.toMap(
                    SyncGroupRequest.MemberAssignment::memberId,
                    member -> member.assignment().partitionAssignments().stream()
                        .map(assignment ->
                            AssignedPartitions.builder()
                                .topicName(assignment.topicName())
                                .partitionIds(assignment.partitionIds())
                                .build()
                        )
                        .collect(Collectors.toList())
                )
            );
    }

    private Assignment createResponseAssignment(List<AssignedPartitions> memberAssignedPartitions) {
        return Assignment.builder()
            .partitionAssignments(
                memberAssignedPartitions.stream()
                    .map(assignedPartitions ->
                        Assignment.PartitionAssignments.builder()
                            .topicName(assignedPartitions.topicName())
                            .partitionIds(assignedPartitions.partitionIds())
                            .build()
                    )
                    .collect(Collectors.toList())
            )
            .build();
    }
}

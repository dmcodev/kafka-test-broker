package dev.dmco.test.kafka.usecase.syncgroup;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.messages.consumer.Assignment;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.Partition;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SyncGroupRequestHandler implements RequestHandler<SyncGroupRequest, SyncGroupResponse> {

    @Override
    public SyncGroupResponse handle(SyncGroupRequest request, BrokerState state) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        ErrorCode memberError = group.checkMemberSynchronization(request.memberId());
        if (memberError != ErrorCode.NO_ERROR) {
            return SyncGroupResponse.builder().errorCode(memberError).build();
        }
        Map<String, List<Partition>> partitionAssignments = extractPartitionAssignments(request, state);
        if (!partitionAssignments.isEmpty()) {
            group.assignPartitions(partitionAssignments);
        }
        group.markSynchronized(request.memberId());
        List<Partition> assignedPartitions = group.getAssignedPartitions(request.memberId());
        return SyncGroupResponse.builder()
            .assignment(createResponseAssignment(assignedPartitions))
            .build();
    }

    private Map<String, List<Partition>> extractPartitionAssignments(SyncGroupRequest request, BrokerState state) {
        return request.memberAssignments().stream()
            .collect(
                Collectors.toMap(
                    SyncGroupRequest.MemberAssignment::memberId,
                    member -> member.assignment().partitionAssignments().stream()
                        .flatMap(assignment ->
                            assignment.partitionIds().stream()
                                .map(partitionId -> state.getTopic(assignment.topicName()).getPartition(partitionId))
                        )
                        .collect(Collectors.toList())
                )
            );
    }

    private Assignment createResponseAssignment(List<Partition> memberAssignedPartitions) {
        Map<String, List<Partition>> partitionsGrouped = memberAssignedPartitions.stream()
            .collect(Collectors.groupingBy(partition -> partition.topic().name()));
        return Assignment.builder()
            .partitionAssignments(
                partitionsGrouped.entrySet().stream()
                    .map(entry ->
                        Assignment.PartitionAssignments.builder()
                            .topicName(entry.getKey())
                            .partitionIds(
                                entry.getValue().stream()
                                    .map(Partition::id)
                                    .sorted()
                                    .collect(Collectors.toList())
                            )
                            .build()
                    )
                    .collect(Collectors.toList())
            )
            .build();
    }
}

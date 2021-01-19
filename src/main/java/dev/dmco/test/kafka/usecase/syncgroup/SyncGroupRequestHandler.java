package dev.dmco.test.kafka.usecase.syncgroup;

import dev.dmco.test.kafka.logging.Logger;
import dev.dmco.test.kafka.messages.consumer.Assignment;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.Partition;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SyncGroupRequestHandler implements RequestHandler<SyncGroupRequest, SyncGroupResponse> {

    private static final Logger LOG = Logger.create(SyncGroupRequestHandler.class);

    @Override
    public void handle(SyncGroupRequest request, BrokerState state, ResponseScheduler<SyncGroupResponse> scheduler) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        Map<String, List<Partition>> partitionAssignments = extractPartitionAssignments(request, state);
        if (!partitionAssignments.isEmpty()) {
            LOG.debug("{}-{} performing assignments: {}", request.groupId(), request.memberId(), partitionAssignments);
            group.assignPartitions(partitionAssignments);
        }
        Collection<Partition> memberPartitions = group.getMember(request.memberId())
            .synchronize();
        LOG.debug("{}-{} synchronized", request.groupId(), request.memberId());
        SyncGroupResponse response = SyncGroupResponse.builder()
            .assignment(createResponseAssignment(memberPartitions))
            .build();
        scheduler.scheduleResponse(response);
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

    private Assignment createResponseAssignment(Collection<Partition> memberAssignedPartitions) {
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

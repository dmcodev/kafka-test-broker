package dev.dmco.test.kafka.usecase.offsetfetch;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OffsetFetchRequestHandler implements RequestHandler<OffsetFetchRequest, OffsetFetchResponse> {

    @Override
    public void handle(OffsetFetchRequest request, BrokerState state, ResponseScheduler<OffsetFetchResponse> scheduler) {
        ConsumerGroup consumerGroup = state.getOrCreateConsumerGroup(request.groupId());
        scheduler.scheduleResponse(
            OffsetFetchResponse.builder()
                .topics(
                    request.topics().stream()
                        .map(requestTopic -> createResponseTopic(requestTopic, consumerGroup))
                        .collect(Collectors.toList())
                )
                .build()
        );
    }

    private OffsetFetchResponse.Topic createResponseTopic(OffsetFetchRequest.Topic requestTopic, ConsumerGroup consumerGroup) {
        String topicName = requestTopic.name();
        Set<Integer> partitionIds = new HashSet<>(requestTopic.partitionIds());
        List<OffsetFetchResponse.Partition> knownResponsePartitions =
            consumerGroup.getPartitionOffsets(topicName).entrySet().stream()
                .filter(entry -> partitionIds.contains(entry.getKey()))
                .map(this::createResponsePartition)
                .collect(Collectors.toList());
        Set<Integer> knownPartitionIds = knownResponsePartitions.stream()
            .map(OffsetFetchResponse.Partition::partitionId)
            .collect(Collectors.toSet());
        List<OffsetFetchResponse.Partition> unknownResponsePartitions = partitionIds.stream()
            .filter(id -> !knownPartitionIds.contains(id))
            .map(this::createResponseZeroPartition)
            .collect(Collectors.toList());
        return OffsetFetchResponse.Topic.builder()
            .name(topicName)
            .partitions(
                Stream.concat(knownResponsePartitions.stream(), unknownResponsePartitions.stream())
                    .collect(Collectors.toList())
            )
            .build();
    }

    private OffsetFetchResponse.Partition createResponsePartition(Map.Entry<Integer, Long> partitionOffset) {
        return OffsetFetchResponse.Partition.builder()
            .partitionId(partitionOffset.getKey())
            .committedOffset(partitionOffset.getValue())
            .build();
    }

    private OffsetFetchResponse.Partition createResponseZeroPartition(int id) {
        return OffsetFetchResponse.Partition.builder()
            .partitionId(id)
            .committedOffset(0L)
            .build();
    }
}

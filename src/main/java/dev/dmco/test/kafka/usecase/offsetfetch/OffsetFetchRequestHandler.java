package dev.dmco.test.kafka.usecase.offsetfetch;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class OffsetFetchRequestHandler implements RequestHandler<OffsetFetchRequest, OffsetFetchResponse> {

    @Override
    public OffsetFetchResponse handle(OffsetFetchRequest request, BrokerState state) {
        ConsumerGroup consumerGroup = state.getConsumerGroup(request.groupId());
        return OffsetFetchResponse.builder()
            .topics(
                request.topics().stream()
                    .map(requestTopic -> createResponseTopic(requestTopic, consumerGroup))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private OffsetFetchResponse.Topic createResponseTopic(OffsetFetchRequest.Topic requestTopic, ConsumerGroup consumerGroup) {
        String topicName = requestTopic.name();
        Set<Integer> partitionIds = new HashSet<>(requestTopic.partitionIds());
        return OffsetFetchResponse.Topic.builder()
            .name(topicName)
            .partitions(
                consumerGroup.getPartitionOffsets(topicName).entrySet().stream()
                    .filter(entry -> partitionIds.contains(entry.getKey()))
                    .map(this::createResponsePartition)
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
}

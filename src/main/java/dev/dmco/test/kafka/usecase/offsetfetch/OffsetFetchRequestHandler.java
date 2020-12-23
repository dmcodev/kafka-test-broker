package dev.dmco.test.kafka.usecase.offsetfetch;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.List;
import java.util.stream.Collectors;

public class OffsetFetchRequestHandler implements RequestHandler<OffsetFetchRequest, OffsetFetchResponse> {

    @Override
    public OffsetFetchResponse handle(OffsetFetchRequest request, BrokerState state) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        return OffsetFetchResponse.builder()
            .topics(
                request.topics().stream()
                    .map(topic ->
                        OffsetFetchResponse.Topic.builder()
                            .name(topic.name())
                            .partitions(createResponsePartitions(group, topic))
                            .build()
                    )
                    .collect(Collectors.toList())
            )
            .build();
    }

    private List<OffsetFetchResponse.Partition> createResponsePartitions(ConsumerGroup group, OffsetFetchRequest.Topic topic) {
        return group.getPartitionOffsets(topic.name()).entrySet().stream()
            .filter(entry -> topic.partitionIds().contains(entry.getKey()))
            .map(entry ->
                OffsetFetchResponse.Partition.builder()
                    .partitionId(entry.getKey())
                    .committedOffset(entry.getValue())
                    .build()
            )
            .collect(Collectors.toList());
    }
}

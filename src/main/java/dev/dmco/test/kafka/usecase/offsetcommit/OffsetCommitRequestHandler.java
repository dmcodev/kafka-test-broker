package dev.dmco.test.kafka.usecase.offsetcommit;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.state.Partition;
import dev.dmco.test.kafka.state.Topic;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.stream.Collectors;

public class OffsetCommitRequestHandler implements RequestHandler<OffsetCommitRequest, OffsetCommitResponse> {

    @Override
    public OffsetCommitResponse handle(OffsetCommitRequest request, BrokerState state) {
        ConsumerGroup consumerGroup = state.getConsumerGroup(request.groupId());
        return OffsetCommitResponse.builder()
            .topics(
                request.topics().stream()
                    .map(requestTopic -> createResponseTopic(requestTopic, state, consumerGroup))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private OffsetCommitResponse.Topic createResponseTopic(
        OffsetCommitRequest.Topic requestTopic,
        BrokerState state,
        ConsumerGroup consumerGroup
    ) {
        Topic topic = state.getTopic(requestTopic.name());
        return OffsetCommitResponse.Topic.builder()
            .name(requestTopic.name())
            .partitions(
                requestTopic.partitions().stream()
                    .map(requestPartition -> createResponsePartition(requestPartition, topic, consumerGroup))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private OffsetCommitResponse.Partition createResponsePartition(
        OffsetCommitRequest.Partition requestPartition,
        Topic topic,
        ConsumerGroup consumerGroup
    ) {
        Partition partition = topic.getPartition(requestPartition.id());
        consumerGroup.commit(partition, requestPartition.committedOffset());
        return OffsetCommitResponse.Partition.builder()
            .id(requestPartition.id())
            .build();
    }
}

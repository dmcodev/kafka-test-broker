package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.Topic;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MetadataRequestHandler implements RequestHandler<MetadataRequest, MetadataResponse> {

    @Override
    public MetadataResponse handle(MetadataRequest request, BrokerState state) {
        return MetadataResponse.builder()
            .broker(createBrokerMetadata(state))
            .topics(createResponseTopics(request.topicNames(), state))
            .build();
    }

    private Collection<MetadataResponse.Topic> createResponseTopics(List<String> topicNames, BrokerState state) {
        return topicNames.stream()
            .map(state::getTopic)
            .map(topic ->
                MetadataResponse.Topic.builder()
                    .name(topic.name())
                    .partitions(createResponsePartitions(topic))
                    .build()
            )
            .collect(Collectors.toList());
    }

    private Collection<MetadataResponse.Partition> createResponsePartitions(Topic topic) {
        return IntStream.range(0, topic.partitionsNumber())
            .mapToObj(partitionId ->
                MetadataResponse.Partition.builder()
                    .id(partitionId)
                    .leaderNodeId(BrokerState.NODE_ID)
                    .build()
            )
            .collect(Collectors.toList());
    }

    private MetadataResponse.Broker createBrokerMetadata(BrokerState state) {
        return MetadataResponse.Broker.builder()
            .nodeId(BrokerState.NODE_ID)
            .host(state.config().host())
            .port(state.config().port())
            .build();
    }
}

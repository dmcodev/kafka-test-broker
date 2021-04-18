package dev.dmcode.test.kafka.usecase.metadata;

import dev.dmcode.test.kafka.config.BrokerConfig;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.Topic;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MetadataRequestHandler implements RequestHandler<MetadataRequest, MetadataResponse> {

    @Override
    public void handle(MetadataRequest request, BrokerState state, ResponseScheduler<MetadataResponse> scheduler) {
        scheduler.scheduleResponse(
            MetadataResponse.builder()
                .broker(createBrokerMetadata(state))
                .topics(createResponseTopics(request.topicNames(), state))
                .build()
        );
    }

    private Collection<MetadataResponse.Topic> createResponseTopics(List<String> topicNames, BrokerState state) {
        return topicNames.stream()
            .map(state::getOrCreateTopic)
            .map(topic ->
                MetadataResponse.Topic.builder()
                    .name(topic.name())
                    .partitions(createResponsePartitions(topic))
                    .build()
            )
            .collect(Collectors.toList());
    }

    private Collection<MetadataResponse.Partition> createResponsePartitions(Topic topic) {
        return IntStream.range(0, topic.numberOfPartitions())
            .mapToObj(partitionId ->
                MetadataResponse.Partition.builder()
                    .id(partitionId)
                    .leaderNodeId(BrokerState.NODE_ID)
                    .build()
            )
            .collect(Collectors.toList());
    }

    private MetadataResponse.Broker createBrokerMetadata(BrokerState state) {
        BrokerConfig config = state.config();
        return MetadataResponse.Broker.builder()
            .nodeId(BrokerState.NODE_ID)
            .host(config.host())
            .port(config.port())
            .build();
    }
}

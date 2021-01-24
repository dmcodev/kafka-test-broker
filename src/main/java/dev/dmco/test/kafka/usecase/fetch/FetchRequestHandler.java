package dev.dmco.test.kafka.usecase.fetch;

import dev.dmco.test.kafka.messages.Record;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.Partition;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

import java.util.List;
import java.util.stream.Collectors;

public class FetchRequestHandler implements RequestHandler<FetchRequest, FetchResponse> {

    @Override
    public void handle(FetchRequest request, BrokerState state, ResponseScheduler<FetchResponse> scheduler) {
        FetchResponse response = fetch(request, state);
        scheduler.scheduleResponse(response);
    }

    private FetchResponse fetch(FetchRequest request, BrokerState state) {
        return FetchResponse.builder()
            .topics(
                request.topics().stream()
                    .map(requestTopic -> fetchTopic(requestTopic, state))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private FetchResponse.Topic fetchTopic(FetchRequest.Topic requestTopic, BrokerState state) {
        return FetchResponse.Topic.builder()
            .name(requestTopic.name())
            .partitions(
                requestTopic.partitions().stream()
                    .map(requestPartition -> fetchPartition(requestTopic.name(), requestPartition, state))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private FetchResponse.Partition fetchPartition(String topicName, FetchRequest.Partition requestPartition, BrokerState state) {
        Partition partition = state.getTopic(topicName).getPartition(requestPartition.partitionId());
        List<Record> records = partition.fetch(requestPartition.fetchOffset(), requestPartition.maxBytes());
        return FetchResponse.Partition.builder()
            .id(partition.id())
            .headOffset(partition.head())
            .records(records)
            .build();
    }
}

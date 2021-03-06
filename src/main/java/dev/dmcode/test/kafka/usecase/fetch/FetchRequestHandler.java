package dev.dmcode.test.kafka.usecase.fetch;

import dev.dmcode.test.kafka.messages.Record;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.Partition;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

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
        Partition partition = state.getOrCreateTopic(topicName).getOrCreatePartition(requestPartition.partitionId());
        List<Record> records = partition.fetch(requestPartition.fetchOffset(), requestPartition.maxBytes());
        return FetchResponse.Partition.builder()
            .id(partition.id())
            .headOffset(partition.headOffset())
            .records(records)
            .build();
    }
}

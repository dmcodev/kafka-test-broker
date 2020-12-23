package dev.dmco.test.kafka.usecase.fetch;

import dev.dmco.test.kafka.messages.Record;
import dev.dmco.test.kafka.messages.Records;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.Partition;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class FetchRequestHandler implements RequestHandler<FetchRequest, FetchResponse> {

    @Override
    public FetchResponse handle(FetchRequest request, BrokerState state) {

        return FetchResponse.builder()
            .topics(
                request.topics().stream()
                    .map(topic ->
                        FetchResponse.Topic.builder()
                            .name(topic.name())
                            .partitions(createResponsePartitions(topic.name(), topic.partitions(), state))
                            .build()
                    )
                    .collect(Collectors.toList())
            )
            .build();
    }

    private Collection<FetchResponse.Partition> createResponsePartitions(String topicName, List<FetchRequest.Partition> partitions, BrokerState state) {
        return partitions.stream()
            .map(partition -> createResponsePartition(topicName, partition, state))
            .collect(Collectors.toList());
    }

    private FetchResponse.Partition createResponsePartition(String topicName, FetchRequest.Partition requestPartition, BrokerState state) {
        Partition partition = state.getTopic(topicName).getPartition(requestPartition.partitionId());
        List<Record> records = partition.fetch(requestPartition.fetchOffset(), requestPartition.maxBytes());
        return FetchResponse.Partition.builder()
            .partitionId(partition.id())
            .highWatermark(partition.head())
            .records(
                Records.builder()
                    .entries(records)
                    .build()
            )
            .build();
    }
}

package dev.dmco.test.kafka.usecase.listoffsets;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ListOffsetsRequestHandler implements RequestHandler<ListOffsetsRequest, ListOffsetsResponse> {

    @Override
    public ListOffsetsResponse handle(ListOffsetsRequest request, BrokerState state) {
        return ListOffsetsResponse.builder()
            .topics(
                request.topics().stream()
                    .map(topic ->
                        ListOffsetsResponse.Topic.builder()
                            .name(topic.name())
                            .partitions(createResponsePartitions(topic, state))
                            .build()
                    )
                    .collect(Collectors.toList())
            )
            .build();
    }

    private List<ListOffsetsResponse.Partition> createResponsePartitions(ListOffsetsRequest.Topic topic, BrokerState state) {
        return topic.partitions().stream()
            .map(partition ->
                ListOffsetsResponse.Partition.builder()
                    .id(partition.id())
                    .offsets(Collections.singletonList(state.getTopic(topic.name()).getPartition(partition.id()).head()))
                    .build()
            )
            .collect(Collectors.toList());
    }
}

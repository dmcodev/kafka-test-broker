package dev.dmco.test.kafka.usecase.produce;

import dev.dmco.test.kafka.messages.Record;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.Partition;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Collection;
import java.util.stream.Collectors;

public class ProduceRequestHandler implements RequestHandler<ProduceRequest, ProduceResponse> {

    @Override
    public ProduceResponse handle(ProduceRequest request, BrokerState state) {
        return ProduceResponse.builder()
            .topics(
                request.topics().stream()
                    .map(targetTopic -> appendToTopic(targetTopic, state))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private ProduceResponse.Topic appendToTopic(ProduceRequest.Topic targetTopic, BrokerState state) {
        return ProduceResponse.Topic.builder()
            .name(targetTopic.name())
            .partitions(
                targetTopic.partitions().stream()
                    .map(targetPartition -> appendToPartition(targetTopic, targetPartition, state))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private ProduceResponse.Partition appendToPartition(
        ProduceRequest.Topic targetTopic,
        ProduceRequest.Partition targetPartition,
        BrokerState state
    ) {
        Collection<Record> records = targetPartition.records().entries();
        Partition.AppendResult appendResult = state.append(targetTopic.name(), targetPartition.id(), records);
        return ProduceResponse.Partition.builder()
            .id(targetPartition.id())
            .errorCode(ResponseMessage.NO_ERROR)
            .baseOffset(appendResult.baseOffset())
            .build();
    }
}

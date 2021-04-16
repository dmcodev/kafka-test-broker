package dev.dmcode.test.kafka.usecase.produce;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.Partition;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

import java.util.stream.Collectors;

public class ProduceRequestHandler implements RequestHandler<ProduceRequest, ProduceResponse> {

    @Override
    public void handle(ProduceRequest request, BrokerState state, ResponseScheduler<ProduceResponse> scheduler) {
        scheduler.scheduleResponse(
            ProduceResponse.builder()
                .topics(
                    request.topics().stream()
                        .map(targetTopic -> appendToTopic(targetTopic, state))
                        .collect(Collectors.toList())
                )
                .build()
        );
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
        Partition.AppendResult result = state.getOrCreateTopic(targetTopic.name())
            .getOrCreatePartition(targetPartition.id())
            .append(targetPartition.records());
        return ProduceResponse.Partition.builder()
            .id(targetPartition.id())
            .errorCode(ErrorCode.NO_ERROR)
            .baseOffset(result.baseOffset())
            .build();
    }
}

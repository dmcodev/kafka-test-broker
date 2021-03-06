package dev.dmcode.test.kafka.usecase.offsetcommit;

import dev.dmcode.test.kafka.logging.Logger;
import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.ConsumerGroup;
import dev.dmcode.test.kafka.state.Partition;
import dev.dmcode.test.kafka.state.Topic;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

import java.util.stream.Collectors;

public class OffsetCommitRequestHandler implements RequestHandler<OffsetCommitRequest, OffsetCommitResponse> {

    private static final Logger LOG = Logger.create(OffsetCommitRequestHandler.class);

    @Override
    public void handle(OffsetCommitRequest request, BrokerState state, ResponseScheduler<OffsetCommitResponse> scheduler) {
        ConsumerGroup consumerGroup = state.getOrCreateConsumerGroup(request.groupId());
        ErrorCode memberError = consumerGroup.validateMember(request.memberId());
        if (memberError != ErrorCode.NO_ERROR) {
            LOG.debug("{}-{} commit error: {}", request.groupId(), request.memberId(), memberError);
        }
        scheduler.scheduleResponse(
            OffsetCommitResponse.builder()
                .topics(
                    request.topics().stream()
                        .map(requestTopic -> createResponseTopic(requestTopic, state, consumerGroup, memberError))
                        .collect(Collectors.toList())
                )
                .build()
        );
    }

    private OffsetCommitResponse.Topic createResponseTopic(
        OffsetCommitRequest.Topic requestTopic,
        BrokerState state,
        ConsumerGroup consumerGroup,
        ErrorCode memberError
    ) {
        Topic topic = state.getOrCreateTopic(requestTopic.name());
        return OffsetCommitResponse.Topic.builder()
            .name(requestTopic.name())
            .partitions(
                requestTopic.partitions().stream()
                    .map(requestPartition -> createResponsePartition(requestPartition, topic, consumerGroup, memberError))
                    .collect(Collectors.toList())
            )
            .build();
    }

    private OffsetCommitResponse.Partition createResponsePartition(
        OffsetCommitRequest.Partition requestPartition,
        Topic topic,
        ConsumerGroup consumerGroup,
        ErrorCode memberError
    ) {
        if (memberError == ErrorCode.NO_ERROR) {
            Partition partition = topic.getOrCreatePartition(requestPartition.id());
            consumerGroup.commit(partition, requestPartition.committedOffset());
        }
        return OffsetCommitResponse.Partition.builder()
            .id(requestPartition.id())
            .errorCode(memberError)
            .build();
    }
}

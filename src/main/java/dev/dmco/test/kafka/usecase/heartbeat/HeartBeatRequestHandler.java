package dev.dmco.test.kafka.usecase.heartbeat;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class HeartBeatRequestHandler implements RequestHandler<HeartBeatRequest, HeartBeatResponse> {

    @Override
    public HeartBeatResponse handle(HeartBeatRequest request, BrokerState state) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        ErrorCode errorCode = group.validateMember(request.memberId());
        return HeartBeatResponse.builder()
            .errorCode(errorCode)
            .build();
    }
}

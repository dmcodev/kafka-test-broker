package dev.dmco.test.kafka.usecase.heartbeat;

import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

public class HeartBeatRequestHandler implements RequestHandler<HeartBeatRequest, HeartBeatResponse> {

    @Override
    public void handle(HeartBeatRequest request, BrokerState state, ResponseScheduler<HeartBeatResponse> scheduler) {
        ConsumerGroup group = state.consumerGroup(request.groupId());
        ErrorCode memberError = group.validateMember(request.memberId());
        scheduler.scheduleResponse(HeartBeatResponse.builder().errorCode(memberError).build());
    }
}

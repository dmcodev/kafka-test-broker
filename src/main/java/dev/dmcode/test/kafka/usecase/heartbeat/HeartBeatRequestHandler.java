package dev.dmcode.test.kafka.usecase.heartbeat;

import dev.dmcode.test.kafka.messages.ErrorCode;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.ConsumerGroup;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

public class HeartBeatRequestHandler implements RequestHandler<HeartBeatRequest, HeartBeatResponse> {

    @Override
    public void handle(HeartBeatRequest request, BrokerState state, ResponseScheduler<HeartBeatResponse> scheduler) {
        ConsumerGroup group = state.getOrCreateConsumerGroup(request.groupId());
        ErrorCode memberError = group.validateMember(request.memberId());
        scheduler.scheduleResponse(HeartBeatResponse.builder().errorCode(memberError).build());
    }
}

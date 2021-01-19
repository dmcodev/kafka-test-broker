package dev.dmco.test.kafka.usecase.heartbeat;

import dev.dmco.test.kafka.logging.Logger;
import dev.dmco.test.kafka.messages.ErrorCode;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

public class HeartBeatRequestHandler implements RequestHandler<HeartBeatRequest, HeartBeatResponse> {

    private static final Logger LOG = Logger.create(HeartBeatRequestHandler.class);

    @Override
    public void handle(HeartBeatRequest request, BrokerState state, ResponseScheduler<HeartBeatResponse> scheduler) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        ErrorCode memberError = group.validateMember(request.memberId());
        if (memberError != ErrorCode.NO_ERROR) {
            LOG.debug("{}-{} heartbeat failed due: {}", request.groupId(), request.groupId(), memberError);
        }
        scheduler.scheduleResponse(HeartBeatResponse.builder().errorCode(memberError).build());
    }
}

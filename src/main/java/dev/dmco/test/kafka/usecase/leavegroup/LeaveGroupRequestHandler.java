package dev.dmco.test.kafka.usecase.leavegroup;

import dev.dmco.test.kafka.logging.Logger;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

public class LeaveGroupRequestHandler implements RequestHandler<LeaveGroupRequest, LeaveGroupResponse> {

    private static final Logger LOG = Logger.create(LeaveGroupRequestHandler.class);

    @Override
    public void handle(LeaveGroupRequest request, BrokerState state, ResponseScheduler<LeaveGroupResponse> scheduler) {
        ConsumerGroup consumerGroup = state.getConsumerGroup(request.groupId());
        consumerGroup.removeMember(request.memberId());
        LOG.debug("{}-{} left consumer group", request.groupId(), request.memberId());
        scheduler.scheduleResponse(LeaveGroupResponse.builder().build());
    }
}

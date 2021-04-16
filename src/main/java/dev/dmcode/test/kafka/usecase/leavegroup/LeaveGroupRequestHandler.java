package dev.dmcode.test.kafka.usecase.leavegroup;

import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.state.ConsumerGroup;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

public class LeaveGroupRequestHandler implements RequestHandler<LeaveGroupRequest, LeaveGroupResponse> {

    @Override
    public void handle(LeaveGroupRequest request, BrokerState state, ResponseScheduler<LeaveGroupResponse> scheduler) {
        ConsumerGroup consumerGroup = state.getOrCreateConsumerGroup(request.groupId());
        consumerGroup.removeMember(request.memberId());
        scheduler.scheduleResponse(LeaveGroupResponse.builder().build());
    }
}

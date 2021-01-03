package dev.dmco.test.kafka.usecase.leavegroup;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class LeaveGroupRequestHandler implements RequestHandler<LeaveGroupRequest, LeaveGroupResponse> {

    @Override
    public LeaveGroupResponse handle(LeaveGroupRequest request, BrokerState state) {
        ConsumerGroup consumerGroup = state.getConsumerGroup(request.groupId());
        consumerGroup.removeMember(request.memberId());
        return LeaveGroupResponse.builder().build();
    }
}

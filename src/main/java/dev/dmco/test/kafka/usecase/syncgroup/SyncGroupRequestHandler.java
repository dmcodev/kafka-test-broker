package dev.dmco.test.kafka.usecase.syncgroup;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.state.ConsumerGroup;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class SyncGroupRequestHandler implements RequestHandler<SyncGroupRequest, SyncGroupResponse> {

    @Override
    public SyncGroupResponse handle(SyncGroupRequest request, BrokerState state) {
        ConsumerGroup group = state.getConsumerGroup(request.groupId());
        return SyncGroupResponse.builder()
            .errorCode((short) 0)
            .assignment(request.memberAssignments().get(0).assignment())
            .build();
    }
}

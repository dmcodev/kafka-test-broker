package dev.dmco.test.kafka.usecase.syncgroup;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Collections;
import java.util.List;

public class SyncGroupRequestHandler implements RequestHandler<SyncGroupRequest, SyncGroupResponse> {

    @Override
    public List<Class<? extends SyncGroupRequest>> handledRequestTypes() {
        return Collections.singletonList(SyncGroupRequest.class);
    }

    @Override
    public SyncGroupResponse handle(SyncGroupRequest request, BrokerState state) {
        return SyncGroupResponse.builder()
            .build();
    }
}

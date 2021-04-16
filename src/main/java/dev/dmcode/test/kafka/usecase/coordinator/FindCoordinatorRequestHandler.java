package dev.dmcode.test.kafka.usecase.coordinator;

import dev.dmcode.test.kafka.config.BrokerConfig;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;

public class FindCoordinatorRequestHandler implements RequestHandler<FindCoordinatorRequest, FindCoordinatorResponse> {

    @Override
    public void handle(FindCoordinatorRequest request, BrokerState state, ResponseScheduler<FindCoordinatorResponse> scheduler) {
        BrokerConfig config = state.getConfig();
        FindCoordinatorResponse response = FindCoordinatorResponse.builder()
            .host(config.host())
            .nodeId(BrokerState.NODE_ID)
            .port(config.port())
            .build();
        scheduler.scheduleResponse(response);
    }
}

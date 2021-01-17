package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.ResponseScheduler;

public class FindCoordinatorRequestHandler implements RequestHandler<FindCoordinatorRequest, FindCoordinatorResponse> {

    @Override
    public void handle(FindCoordinatorRequest request, BrokerState state, ResponseScheduler<FindCoordinatorResponse> scheduler) {
        scheduler.scheduleResponse(
            FindCoordinatorResponse.builder()
                .host(state.config().host())
                .nodeId(BrokerState.NODE_ID)
                .port(state.config().port())
                .build()
        );
    }
}

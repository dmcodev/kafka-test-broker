package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class FindCoordinatorRequestHandler implements RequestHandler<FindCoordinatorRequest, FindCoordinatorResponse> {

    @Override
    public FindCoordinatorResponse handle(FindCoordinatorRequest request, BrokerState state) {
        return FindCoordinatorResponse.builder()
            .host(state.config().host())
            .nodeId(BrokerState.NODE_ID)
            .port(state.config().port())
            .build();
    }
}

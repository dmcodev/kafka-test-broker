package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class FindCoordinatorRequestHandler implements RequestHandler<FindCoordinatorRequest, FindCoordinatorResponse> {

    @Override
    public FindCoordinatorResponse handle(FindCoordinatorRequest request, BrokerState state) {
        return FindCoordinatorResponse.builder()
            .errorCode((short) 0)
            .host("localhost")
            .nodeId(0)
            .port(9092)
            .throttleTimeMs(0)
            .build();
    }
}

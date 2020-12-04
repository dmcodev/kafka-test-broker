package dev.dmco.test.kafka.usecase.coordinator;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.List;

import static java.util.Collections.singletonList;

public class FindCoordinatorRequestHandler implements RequestHandler<FindCoordinatorRequest, FindCoordinatorResponse> {

    @Override
    public List<Class<? extends FindCoordinatorRequest>> handledRequestTypes() {
        return singletonList(FindCoordinatorRequest.class);
    }

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

package dev.dmco.test.kafka.handlers;

import dev.dmco.test.kafka.messages.ApiKeys;
import dev.dmco.test.kafka.messages.ApiVersionsRequest;
import dev.dmco.test.kafka.messages.ApiVersionsResponse;
import dev.dmco.test.kafka.messages.ApiVersionsResponse.ApiKey;
import dev.dmco.test.kafka.state.BrokerState;

import java.util.List;

import static java.util.Collections.singletonList;

public class ApiVersionsRequestHandler implements RequestHandler<ApiVersionsRequest, ApiVersionsResponse> {

    @Override
    public List<Class<? extends ApiVersionsRequest>> handledRequestTypes() {
        return singletonList(ApiVersionsRequest.class);
    }

    @Override
    public ApiVersionsResponse handle(ApiVersionsRequest request, BrokerState state) {
        return ApiVersionsResponse.builder()
            .errorCode(0)
            .apiKey(ApiKey.from(ApiKeys.API_VERSIONS_KEY, 0, 0))
            .build();
    }
}

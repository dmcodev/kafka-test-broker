package dev.dmco.test.kafka.handlers;

import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.request.ApiVersionsRequest;
import dev.dmco.test.kafka.messages.response.ApiVersionsResponse;
import dev.dmco.test.kafka.messages.response.ApiVersionsResponse.ApiKey;
import dev.dmco.test.kafka.state.BrokerState;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

public class ApiVersionsRequestHandler implements RequestHandler<ApiVersionsRequest, ApiVersionsResponse> {

    @Override
    public List<Class<? extends ApiVersionsRequest>> handledRequestTypes() {
        return singletonList(ApiVersionsRequest.class);
    }

    @Override
    public ApiVersionsResponse handle(ApiVersionsRequest request, BrokerState state) {
        return ApiVersionsResponse.builder()
            .errorCode((short) 0)
            .apiKeys(
                state.handlersRegistry().getHandlers().stream()
                    .map(RequestHandler::handledRequestTypes)
                    .flatMap(Collection::stream)
                    .filter(type -> type.isAnnotationPresent(Request.class))
                    .map(type -> type.getAnnotation(Request.class))
                    .map(metadata ->
                        ApiKey.builder()
                            .apiKey((short) metadata.apiKey())
                            .minVersion((short) 0)
                            .maxVersion((short) metadata.maxVersion())
                            .build()
                    )
                    .collect(Collectors.toList())
            )
            .build();
    }
}

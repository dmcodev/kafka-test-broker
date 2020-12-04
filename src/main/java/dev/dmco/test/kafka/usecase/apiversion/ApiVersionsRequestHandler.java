package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.apiversion.ApiVersionsResponse.ApiKey;

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
                RequestHandler.loadAll().stream()
                    .map(RequestHandler::handledRequestTypes)
                    .flatMap(Collection::stream)
                    .filter(type -> type.isAnnotationPresent(Request.class))
                    .map(this::createApiKey)
                    .collect(Collectors.toList())
            )
            .build();
    }

    private ApiKey createApiKey(Class<?> requestType) {
        Request metadata = requestType.getAnnotation(Request.class);
        return ApiKey.builder()
            .apiKey((short) metadata.key())
            .minVersion((short) 0)
            .maxVersion((short) metadata.maxVersion())
            .build();
    }
}

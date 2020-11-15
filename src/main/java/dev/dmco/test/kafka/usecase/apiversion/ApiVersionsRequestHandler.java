package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.messages.RequestHandler;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.state.BrokerState;
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
        if (!requestType.isAnnotationPresent(ApiVersion.class)) {
            throw new IllegalStateException("Supported api version not defined for: " + requestType);
        }
        int apiKey = requestType.getAnnotation(Request.class).apiKey();
        ApiVersion apiVersion = requestType.getAnnotation(ApiVersion.class);
        return ApiKey.builder()
            .apiKey((short) apiKey)
            .minVersion((short) apiVersion.min())
            .maxVersion((short) apiVersion.max())
            .build();
    }
}

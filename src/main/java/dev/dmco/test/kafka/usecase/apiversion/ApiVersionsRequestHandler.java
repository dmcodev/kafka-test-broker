package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;
import dev.dmco.test.kafka.usecase.apiversion.ApiVersionsResponse.ApiKey;

import java.util.stream.Collectors;

public class ApiVersionsRequestHandler implements RequestHandler<ApiVersionsRequest, ApiVersionsResponse> {

    @Override
    public ApiVersionsResponse handle(ApiVersionsRequest request, BrokerState state) {
        return ApiVersionsResponse.builder()
            .errorCode((short) 0)
            .apiKeys(
                RequestHandler.loadAll().stream()
                    .map(RequestHandler::getHandledRequestType)
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

package dev.dmcode.test.kafka.usecase.apiversion;

import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.state.BrokerState;
import dev.dmcode.test.kafka.usecase.RequestHandler;
import dev.dmcode.test.kafka.usecase.ResponseScheduler;
import dev.dmcode.test.kafka.usecase.apiversion.ApiVersionsResponse.ApiKey;

import java.util.stream.Collectors;

public class ApiVersionsRequestHandler implements RequestHandler<ApiVersionsRequest, ApiVersionsResponse> {

    @Override
    public void handle(ApiVersionsRequest request, BrokerState state, ResponseScheduler<ApiVersionsResponse> scheduler) {
        scheduler.scheduleResponse(
            ApiVersionsResponse.builder()
                .apiKeys(
                    state.getRequestHandlers().handledRequestTypes().stream()
                        .map(type -> type.getAnnotation(Request.class))
                        .map(this::createApiKey)
                        .collect(Collectors.toList())
                )
                .build()
        );
    }

    private ApiKey createApiKey(Request metadata) {
        return ApiKey.builder()
            .apiKey((short) metadata.key())
            .minVersion((short) metadata.minVersion())
            .maxVersion((short) metadata.maxVersion())
            .build();
    }
}

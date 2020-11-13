package dev.dmco.test.kafka.handlers;

import dev.dmco.test.kafka.messages.request.MetadataRequest;
import dev.dmco.test.kafka.messages.response.MetadataResponse;
import dev.dmco.test.kafka.state.BrokerState;

import java.util.Collections;
import java.util.List;

public class MetadataRequestHandler implements RequestHandler<MetadataRequest, MetadataResponse> {

    @Override
    public List<Class<? extends MetadataRequest>> handledRequestTypes() {
        return Collections.singletonList(MetadataRequest.class);
    }

    @Override
    public MetadataResponse handle(MetadataRequest request, BrokerState state) {
        return new MetadataResponse();
    }
}

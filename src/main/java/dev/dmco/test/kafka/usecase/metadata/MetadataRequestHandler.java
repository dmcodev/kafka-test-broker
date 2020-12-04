package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Collections;
import java.util.List;

public class MetadataRequestHandler implements RequestHandler<MetadataRequest, MetadataResponse> {

    @Override
    public List<Class<? extends MetadataRequest>> handledRequestTypes() {
        return Collections.singletonList(MetadataRequest.class);
    }

    @Override
    public MetadataResponse handle(MetadataRequest request, BrokerState state) {
        return MetadataResponse.builder()
            .broker(
                MetadataResponse.Broker.builder()
                    .nodeId(state.nodeId())
                    .host(state.config().host())
                    .port(state.config().port())
                    .build()
            )
            .topic(
                MetadataResponse.Topic.builder()
                    .errorCode((short) 0)
                    .name("my-topic")
                    .isInternal(false)
                    .partition(
                        MetadataResponse.Partition.builder()
                            .errorCode((short) 0)
                            .partitionIndex(0)
                            .leaderId(state.nodeId())
                            .build()
                    )
                    .build()
            )
            .build();
    }
}

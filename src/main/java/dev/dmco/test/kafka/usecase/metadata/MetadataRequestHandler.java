package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class MetadataRequestHandler implements RequestHandler<MetadataRequest, MetadataResponse> {

    private static final int NODE_ID = 1;

    @Override
    public MetadataResponse handle(MetadataRequest request, BrokerState state) {
        return MetadataResponse.builder()
            .broker(
                MetadataResponse.Broker.builder()
                    .nodeId(NODE_ID)
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
                            .leaderId(NODE_ID)
                            .build()
                    )
                    .build()
            )
            .build();
    }
}

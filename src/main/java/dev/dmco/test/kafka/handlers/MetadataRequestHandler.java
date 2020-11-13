package dev.dmco.test.kafka.handlers;

import dev.dmco.test.kafka.messages.request.MetadataRequest;
import dev.dmco.test.kafka.messages.response.MetadataResponse;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
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
        return MetadataResponse.builder()
            .header(
                ResponseHeader.builder()
                    .correlationId(request.header().correlationId())
                    .build()
            )
            .broker(
                MetadataResponse.Broker.builder()
                    .nodeId(7872)
                    .host("localhost")
                    .port(9092)
                    .rack("rack")
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
                            .leaderId(7872)
                            .build()
                    )
                    .build()
            )
            .build();
    }
}

package dev.dmco.test.kafka.handlers;

import dev.dmco.test.kafka.messages.request.ProduceRequest;
import dev.dmco.test.kafka.messages.response.ProduceResponse;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.state.BrokerState;

import java.util.List;

import static java.util.Collections.singletonList;

public class ProduceRequestHandler implements RequestHandler<ProduceRequest, ProduceResponse> {

    @Override
    public List<Class<? extends ProduceRequest>> handledRequestTypes() {
        return singletonList(ProduceRequest.class);
    }

    @Override
    public ProduceResponse handle(ProduceRequest request, BrokerState state) {
        return ProduceResponse.builder()
            .header(
                ResponseHeader.builder()
                    .correlationId(request.header().correlationId())
                    .build()
            )
            .topic(
                ProduceResponse.Topic.builder()
                    .name("my-topic")
                    .partition(
                        ProduceResponse.Partition.builder()
                            .partition(0)
                            .errorCode((short) 0)
                            .baseOffset(1L)
                            .build()
                    )
                    .build()
            )
            .build();
    }
}

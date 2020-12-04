package dev.dmco.test.kafka.usecase.produce;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

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

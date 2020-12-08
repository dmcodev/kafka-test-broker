package dev.dmco.test.kafka.usecase.offsetfetch;

import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class OffsetFetchRequestHandler implements RequestHandler<OffsetFetchRequest, OffsetFetchResponse> {

    @Override
    public OffsetFetchResponse handle(OffsetFetchRequest request, BrokerState state) {
        return OffsetFetchResponse.builder()
            .topic(
                OffsetFetchResponse.Topic.builder()
                    .name("my-topic")
                    .partition(
                        OffsetFetchResponse.Partition.builder()
                            .partitionId(0)
                            .committedOffset(0)
                            .metadata(null)
                            .errorCode((short) 0)
                            .build()
                    )
                    .build()
            )
            .build();
    }
}

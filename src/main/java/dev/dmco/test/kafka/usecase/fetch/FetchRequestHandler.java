package dev.dmco.test.kafka.usecase.fetch;

import dev.dmco.test.kafka.messages.Record;
import dev.dmco.test.kafka.messages.Records;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

public class FetchRequestHandler implements RequestHandler<FetchRequest, FetchResponse> {

    @Override
    public FetchResponse handle(FetchRequest request, BrokerState state) {
        return FetchResponse.builder()
            .topic(
                FetchResponse.Topic.builder()
                    .name("my-topic")
                    .partition(
                        FetchResponse.Partition.builder()
                            .partitionId(0)
                            .errorCode((short) 0)
                            .highWatermark(2)
                            .records(
                                Records.builder()
                                    .entry(
                                        Record.builder()
                                            .offset(0)
                                            .key("some-key".getBytes())
                                            .value("some-value".getBytes())
                                            .build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();
    }
}

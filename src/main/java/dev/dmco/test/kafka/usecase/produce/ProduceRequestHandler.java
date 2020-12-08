package dev.dmco.test.kafka.usecase.produce;

import dev.dmco.test.kafka.messages.Record;
import dev.dmco.test.kafka.state.BrokerState;
import dev.dmco.test.kafka.usecase.RequestHandler;

import java.util.Collection;

public class ProduceRequestHandler implements RequestHandler<ProduceRequest, ProduceResponse> {

    @Override
    public ProduceResponse handle(ProduceRequest request, BrokerState state) {
        request.topics()
            .forEach(topic -> appendTopic(topic, state));
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

    private void appendTopic(ProduceRequest.Topic topic, BrokerState state) {
        topic.partitions()
            .forEach(partition -> appendPartition(topic, partition, state));
    }

    private void appendPartition(ProduceRequest.Topic topic, ProduceRequest.Partition partition, BrokerState state) {
        Collection<Record> records = partition.records().entries();
        state.append(topic.name(), partition.id(), records);
    }
}

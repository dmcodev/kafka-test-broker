package dev.dmco.test.kafka.usecase.produce;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.List;

@Request(apiKey = 0)
@ApiVersion(max = 2)
@HeaderVersion(value = 1, sinceApiVersion = 0)
@lombok.Value
@Accessors(fluent = true)
public class ProduceRequest implements RequestMessage {

    RequestHeader header;

    @ApiVersion(min = 3)
    String transactionalId;

    short acks;
    int timeout;

    List<TopicData> topicData;

    @lombok.Value
    @Accessors(fluent = true)
    public static class TopicData {

        @Value(ValueType.STRING)
        String topic;

        List<PartitionRecordSet> data;
    }

    @lombok.Value
    @Accessors(fluent = true)
    public static class PartitionRecordSet {

        int partition;

        @Value(ValueType.RECORDS)
        List<Record> records;
    }

    @lombok.Value
    @Builder
    @Accessors(fluent = true)
    public static class Record {
        byte[] key;
        byte[] value;
    }
}

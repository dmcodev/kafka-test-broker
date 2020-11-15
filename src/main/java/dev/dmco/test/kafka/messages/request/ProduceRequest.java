package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.common.Records;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Accessors(fluent = true)
@Request(apiKey = 0, maxVersion = 2)
public class ProduceRequest implements RequestMessage {

    @ApiVersionOverride(value = 1, sinceVersion = 0)
    RequestHeader header;

    @ApiVersion(min = 3)
    @Value(ValueType.NULLABLE_STRING)
    String transactionalId;

    @Value(ValueType.INT16)
    Short acks;

    @Value(ValueType.INT32)
    Integer timeout;

    @StructSequence(TopicData.class)
    List<TopicData> topicData;

    @lombok.Value
    @Accessors(fluent = true)
    public static class TopicData {

        @Value(ValueType.STRING)
        String topic;

        @StructSequence(PartitionRecordSet.class)
        List<PartitionRecordSet> data;
    }

    @lombok.Value
    @Accessors(fluent = true)
    public static class PartitionRecordSet {

        @Value(ValueType.INT32)
        Integer partition;

        @Value(ValueType.RECORDS)
        Records records;
    }
}

package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.common.Records;
import dev.dmco.test.kafka.messages.meta.Field;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.SinceVersion;
import dev.dmco.test.kafka.messages.meta.Struct;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.VersionOverride;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Accessors(fluent = true)
@Request(apiKey = 0, maxVersion = 3)
public class ProduceRequest implements RequestMessage {

    @Struct
    @VersionOverride(value = 1, sinceVersion = 0)
    RequestHeader header;

    @SinceVersion(3)
    @Field(FieldType.NULLABLE_STRING)
    String transactionalId;

    @Field(FieldType.INT16)
    Short acks;

    @Field(FieldType.INT32)
    Integer timeout;

    @StructSequence(TopicData.class)
    List<TopicData> topicData;

    @Value
    @Accessors(fluent = true)
    public static class TopicData {

        @Field(FieldType.STRING)
        String topic;

        @StructSequence(PartitionRecordSet.class)
        List<PartitionRecordSet> data;
    }

    @Value
    @Accessors(fluent = true)
    public static class PartitionRecordSet {

        @Field(FieldType.INT32)
        Integer partition;

        @Field(FieldType.RECORDS)
        Records records;
    }
}

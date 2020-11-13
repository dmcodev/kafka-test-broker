package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.io.struct.FieldType;
import dev.dmco.test.kafka.messages.ResponseMessage;
import dev.dmco.test.kafka.messages.meta.Field;
import dev.dmco.test.kafka.messages.meta.FieldSequence;
import dev.dmco.test.kafka.messages.meta.SinceVersion;
import dev.dmco.test.kafka.messages.meta.Struct;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.VersionOverride;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;

import java.util.List;

@Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class MetadataResponse implements ResponseMessage {

    @Struct
    @VersionOverride(value = 0, sinceVersion = 0)
    @VersionOverride(value = 1, sinceVersion = 9)
    ResponseHeader header;

    @StructSequence(Broker.class)
    @Singular
    List<Broker> brokers;

    @SinceVersion(1)
    @Field(FieldType.INT32)
    Integer controllerId;

    @StructSequence(Topic.class)
    @Singular
    List<Topic> topics;

    @Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Broker {

        @Field(FieldType.INT32)
        Integer nodeId;

        @Field(FieldType.STRING)
        String host;

        @Field(FieldType.INT32)
        Integer port;

        @SinceVersion(1)
        @Field(FieldType.NULLABLE_STRING)
        String rack;
    }

    @Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Topic {

        @Field(FieldType.INT16)
        Short errorCode;

        @Field(FieldType.STRING)
        String name;

        @Field(FieldType.BOOLEAN)
        Boolean isInternal;

        @StructSequence(Partition.class)
        @Singular
        List<Partition> partitions;
    }

    @Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Partition {

        @Field(FieldType.INT16)
        Short errorCode;

        @Field(FieldType.INT32)
        Integer partitionIndex;

        @Field(FieldType.INT32)
        Integer leaderId;

        @FieldSequence(FieldType.INT32)
        @Singular
        List<Integer> replicaNodes;

        @FieldSequence(FieldType.INT32)
        @Singular
        List<Integer> isrNodes;
    }
}

package dev.dmco.test.kafka.messages.response;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.meta.ValueSequence;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.experimental.Accessors;

import java.util.List;

@lombok.Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class MetadataResponse implements ResponseMessage {

    @ApiVersionOverride(value = 0, sinceVersion = 0)
    @ApiVersionOverride(value = 1, sinceVersion = 9)
    ResponseHeader header;

    @StructSequence(Broker.class)
    @Singular
    List<Broker> brokers;

    @ApiVersion(min = 1)
    @Value(ValueType.INT32)
    Integer controllerId;

    @StructSequence(Topic.class)
    @Singular
    List<Topic> topics;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Broker {

        @Value(ValueType.INT32)
        Integer nodeId;

        @Value(ValueType.STRING)
        String host;

        @Value(ValueType.INT32)
        Integer port;

        @ApiVersion(min = 1)
        @Value(ValueType.NULLABLE_STRING)
        String rack;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Topic {

        @Value(ValueType.INT16)
        Short errorCode;

        @Value(ValueType.STRING)
        String name;

        @ApiVersion(min = 1)
        @Value(ValueType.BOOLEAN)
        Boolean isInternal;

        @StructSequence(Partition.class)
        @Singular
        List<Partition> partitions;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Partition {

        @Value(ValueType.INT16)
        Short errorCode;

        @Value(ValueType.INT32)
        Integer partitionIndex;

        @Value(ValueType.INT32)
        Integer leaderId;

        @ValueSequence(ValueType.INT32)
        @Singular
        List<Integer> replicaNodes;

        @ValueSequence(ValueType.INT32)
        @Singular
        List<Integer> isrNodes;
    }
}

package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.StructSequence;
import dev.dmco.test.kafka.messages.meta.Value;
import dev.dmco.test.kafka.messages.meta.ValueSequence;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.experimental.Accessors;

import java.util.List;

@HeaderVersion(value = 0, sinceApiVersion = 0)
@HeaderVersion(value = 1, sinceApiVersion = 9)
@lombok.Value
@Builder
@AllArgsConstructor
@Accessors(fluent = true)
public class MetadataResponse implements ResponseMessage {

    @Value(ValueType.RESPOSNE_HEADER)
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

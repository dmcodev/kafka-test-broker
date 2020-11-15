package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.io.codec.value.ValueType;
import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.Value;
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

    ResponseHeader header;

    @Singular
    List<Broker> brokers;

    @ApiVersion(min = 1)
    int controllerId;

    @Singular
    List<Topic> topics;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Broker {

        int nodeId;

        @Value(ValueType.STRING)
        String host;

        int port;

        @ApiVersion(min = 1)
        String rack;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Topic {

        short errorCode;

        @Value(ValueType.STRING)
        String name;

        @ApiVersion(min = 1)
        boolean isInternal;

        @Singular
        List<Partition> partitions;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Partition {

        short errorCode;
        int partitionIndex;
        int leaderId;

        @Singular
        List<Integer> replicaNodes;

        @Singular
        List<Integer> isrNodes;
    }
}

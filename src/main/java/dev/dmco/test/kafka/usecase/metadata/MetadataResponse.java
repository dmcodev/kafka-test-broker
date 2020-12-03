package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.messages.meta.ApiVersionMapping;
import dev.dmco.test.kafka.messages.meta.SinceApiVersion;
import dev.dmco.test.kafka.messages.response.ResponseHeader;
import dev.dmco.test.kafka.messages.response.ResponseMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Singular;
import lombok.With;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

@lombok.Value
@Builder
@With
@AllArgsConstructor
@Accessors(fluent = true)
public class MetadataResponse implements ResponseMessage {

    @ApiVersionMapping(value = 0, sinceApiVersion = 0)
    @ApiVersionMapping(value = 1, sinceApiVersion = 9)
    ResponseHeader header;

    @Singular
    List<Broker> brokers;

    @SinceApiVersion(2)
    Optional<String> clusterId;

    @SinceApiVersion(1)
    int controllerId;

    @Singular
    List<Topic> topics;

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Broker {

        int nodeId;

        String host;

        int port;

        @SinceApiVersion(1)
        Optional<String> rack;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Topic {

        short errorCode;

        String name;

        @SinceApiVersion(1)
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

package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
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

    @VersionMapping(value = 0, sinceVersion = 0)
    @VersionMapping(value = 1, sinceVersion = 9)
    ResponseHeader header;

    @Singular
    List<Broker> brokers;

    @SinceVersion(2)
    Optional<String> clusterId;

    @SinceVersion(1)
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

        @SinceVersion(1)
        Optional<String> rack;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Topic {

        short errorCode;

        String name;

        @SinceVersion(1)
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

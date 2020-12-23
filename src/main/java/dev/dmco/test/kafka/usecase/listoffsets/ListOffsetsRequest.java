package dev.dmco.test.kafka.usecase.listoffsets;

import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;

@Request(key = 2)
@lombok.Value
@Accessors(fluent = true)
public class ListOffsetsRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    RequestHeader header;

    int replicaId;

    List<Topic> topics;

    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Topic {

        String name;

        List<Partition> partitions;
    }

    @lombok.Value
    @AllArgsConstructor
    @Accessors(fluent = true)
    public static class Partition {

        int id;

        long timestamp;

        int maxNumberOfOffsets;
    }
}

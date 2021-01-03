package dev.dmco.test.kafka.usecase.offsetcommit;

import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

@Request(key = 8)
@lombok.Value
@Accessors(fluent = true)
public class OffsetCommitRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 8)
    RequestHeader header;

    String groupId;

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

        long committedOffset;

        Optional<String> metadata;
    }
}

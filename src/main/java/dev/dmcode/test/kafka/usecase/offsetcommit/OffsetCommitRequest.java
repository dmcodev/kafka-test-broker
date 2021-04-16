package dev.dmcode.test.kafka.usecase.offsetcommit;

import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

@Request(key = 8, minVersion = 1, maxVersion = 1)
@lombok.Value
@Accessors(fluent = true)
public class OffsetCommitRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 8)
    RequestHeader header;

    String groupId;

    @SinceVersion(1)
    int generationId;

    @SinceVersion(1)
    String memberId;

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

        @SinceVersion(1)
        long commitTimestamp;

        Optional<String> metadata;
    }
}

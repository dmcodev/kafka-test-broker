package dev.dmcode.test.kafka.usecase.fetch;

import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.List;

@Request(key = 1, maxVersion = 4)
@lombok.Value
@Accessors(fluent = true)
public class FetchRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    RequestHeader header;

    int replicaId;

    int maxWaitTime;

    int minBytes;

    @SinceVersion(3)
    int maxBytes;

    @SinceVersion(4)
    byte isolationLevel;

    List<Topic> topics;

    @lombok.Value
    @Accessors(fluent = true)
    public static class Topic {

        String name;

        List<Partition> partitions;
    }

    @lombok.Value
    @Builder
    @AllArgsConstructor
    public static class Partition {

        int partitionId;

        long fetchOffset;

        int maxBytes;
    }
}

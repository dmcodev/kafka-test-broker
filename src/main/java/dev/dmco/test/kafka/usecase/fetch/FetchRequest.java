package dev.dmco.test.kafka.usecase.fetch;

import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.Accessors;

import java.util.List;

@Request(key = 1, maxVersion = 3)
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

package dev.dmcode.test.kafka.usecase.produce;

import dev.dmcode.test.kafka.messages.Record;
import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

@Request(key = 0, maxVersion = 3)
@lombok.Value
@Accessors(fluent = true)
public class ProduceRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    RequestHeader header;

    @SinceVersion(3)
    Optional<String> transactionalId;

    short requiredAcknowledgments;

    int timeoutMs;

    List<Topic> topics;

    @lombok.Value
    @Accessors(fluent = true)
    public static class Topic {

        String name;

        List<Partition> partitions;
    }

    @lombok.Value
    @Accessors(fluent = true)
    public static class Partition {

        int id;

        @VersionMapping(value = 0, sinceVersion = 0)
        @VersionMapping(value = 1, sinceVersion = 2)
        @VersionMapping(value = 2, sinceVersion = 3)
        Collection<Record> records;
    }
}

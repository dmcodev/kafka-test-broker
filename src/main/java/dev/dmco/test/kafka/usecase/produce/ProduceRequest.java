package dev.dmco.test.kafka.usecase.produce;

import dev.dmco.test.kafka.messages.Records;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.List;
import java.util.Optional;

// TODO: magic version 2 starts from api version 3
@Request(key = 0, maxVersion = 2)
@lombok.Value
@Accessors(fluent = true)
public class ProduceRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    RequestHeader header;

    @SinceVersion(3)
    Optional<String> transactionalId;

    short requiredAcknowledgments;

    int timeout;

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

        Records records;
    }
}

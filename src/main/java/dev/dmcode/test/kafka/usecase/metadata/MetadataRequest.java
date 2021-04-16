package dev.dmcode.test.kafka.usecase.metadata;

import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.SinceVersion;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.List;

@Request(key = 3, maxVersion = 2)
@lombok.Value
@Accessors(fluent = true)
public class MetadataRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 9)
    RequestHeader header;

    List<String> topicNames;

    @SinceVersion(4)
    boolean allowAutoTopicCreation;

    @SinceVersion(8)
    boolean includeClusterAuthorizedOperations;

    @SinceVersion(8)
    boolean includeTopicAuthorizedOperations;
}

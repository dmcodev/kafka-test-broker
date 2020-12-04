package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.messages.metadata.ApiVersion;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.SinceVersion;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.List;

@Request(apiKey = 3)
@ApiVersion(max = 2)
@lombok.Value
@Accessors(fluent = true)
public class MetadataRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceApiVersion = 0)
    @VersionMapping(value = 2, sinceApiVersion = 9)
    RequestHeader header;

    List<String> topicNames;

    @SinceVersion(4)
    boolean allowAutoTopicCreation;

    @SinceVersion(8)
    boolean includeClusterAuthorizedOperations;

    @SinceVersion(8)
    boolean includeTopicAuthorizedOperations;
}

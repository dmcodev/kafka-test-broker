package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.List;

@Request(apiKey = 3)
@ApiVersion(max = 2)
@HeaderVersion(value = 1, sinceApiVersion = 0)
@HeaderVersion(value = 2, sinceApiVersion = 9)
@lombok.Value
@Accessors(fluent = true)
public class MetadataRequest implements RequestMessage {

    RequestHeader header;

    List<String> topicNames;

    @ApiVersion(min = 4)
    boolean allowAutoTopicCreation;

    @ApiVersion(min = 8)
    boolean includeClusterAuthorizedOperations;

    @ApiVersion(min = 8)
    boolean includeTopicAuthorizedOperations;
}

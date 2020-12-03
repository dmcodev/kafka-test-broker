package dev.dmco.test.kafka.usecase.metadata;

import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.ApiVersionMapping;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.SinceApiVersion;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

import java.util.List;

@Request(apiKey = 3)
@ApiVersion(max = 2)
@lombok.Value
@Accessors(fluent = true)
public class MetadataRequest implements RequestMessage {

    @ApiVersionMapping(value = 1, sinceApiVersion = 0)
    @ApiVersionMapping(value = 2, sinceApiVersion = 9)
    RequestHeader header;

    List<String> topicNames;

    @SinceApiVersion(4)
    boolean allowAutoTopicCreation;

    @SinceApiVersion(8)
    boolean includeClusterAuthorizedOperations;

    @SinceApiVersion(8)
    boolean includeTopicAuthorizedOperations;
}

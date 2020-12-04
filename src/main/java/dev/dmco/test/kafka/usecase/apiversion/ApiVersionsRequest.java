package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.messages.metadata.ApiVersion;
import dev.dmco.test.kafka.messages.metadata.Request;
import dev.dmco.test.kafka.messages.metadata.VersionMapping;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

@Request(apiKey = 18)
@ApiVersion(max = 3)
@lombok.Value
@Accessors(fluent = true)
public class ApiVersionsRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceApiVersion = 0)
    @VersionMapping(value = 2, sinceApiVersion = 3)
    RequestHeader header;
}

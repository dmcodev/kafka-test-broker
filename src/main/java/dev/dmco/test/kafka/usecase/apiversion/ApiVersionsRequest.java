package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.Value;
import lombok.experimental.Accessors;

@Request(apiKey = 18)
@ApiVersion(max = 3)
@Value
@Accessors(fluent = true)
public class ApiVersionsRequest implements RequestMessage {

    @ApiVersionOverride(value = 1, sinceVersion = 0)
    @ApiVersionOverride(value = 2, sinceVersion = 3)
    RequestHeader header;
}

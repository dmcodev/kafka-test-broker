package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.messages.meta.ApiVersionOverride;
import dev.dmco.test.kafka.messages.meta.Request;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@Request(apiKey = 18)
public class ApiVersionsRequest implements RequestMessage {

    @ApiVersionOverride(value = 1, sinceVersion = 0)
    @ApiVersionOverride(value = 2, sinceVersion = 3)
    RequestHeader header;
}

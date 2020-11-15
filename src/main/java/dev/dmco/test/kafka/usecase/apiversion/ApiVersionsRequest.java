package dev.dmco.test.kafka.usecase.apiversion;

import dev.dmco.test.kafka.messages.meta.ApiVersion;
import dev.dmco.test.kafka.messages.meta.HeaderVersion;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.request.RequestHeader;
import dev.dmco.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

@Request(apiKey = 18)
@ApiVersion(max = 3)
@HeaderVersion(value = 1, sinceApiVersion = 0)
@HeaderVersion(value = 2, sinceApiVersion = 3)
@lombok.Value
@Accessors(fluent = true)
public class ApiVersionsRequest implements RequestMessage {

    RequestHeader header;
}

package dev.dmco.test.kafka.messages.request;

import dev.dmco.test.kafka.messages.RequestMessage;
import dev.dmco.test.kafka.messages.meta.Request;
import dev.dmco.test.kafka.messages.meta.Struct;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@Request(apiKey = 18)
public class ApiVersionsRequest implements RequestMessage {

    @Struct
    RequestHeader header;
}

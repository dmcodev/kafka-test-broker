package dev.dmcode.test.kafka.usecase.apiversion;

import dev.dmcode.test.kafka.messages.metadata.Request;
import dev.dmcode.test.kafka.messages.metadata.VersionMapping;
import dev.dmcode.test.kafka.messages.request.RequestHeader;
import dev.dmcode.test.kafka.messages.request.RequestMessage;
import lombok.experimental.Accessors;

@Request(key = 18, maxVersion = 3)
@lombok.Value
@Accessors(fluent = true)
public class ApiVersionsRequest implements RequestMessage {

    @VersionMapping(value = 1, sinceVersion = 0)
    @VersionMapping(value = 2, sinceVersion = 3)
    RequestHeader header;
}

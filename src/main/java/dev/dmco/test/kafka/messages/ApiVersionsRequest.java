package dev.dmco.test.kafka.messages;

import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
public class ApiVersionsRequest implements RequestMessage {
    RequestHeader header;
}

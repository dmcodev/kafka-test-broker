package dev.dmco.test.kafka.messages;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@KafkaRequest(apiKey = 18)
public class ApiVersionsRequest implements RequestMessage {
    RequestHeader header;
}
